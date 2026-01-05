using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;



/*
 * SyncAgent 全功能整合版 v3
 * -----------------------------
 * - appsettings.json 配置
 * - 多节点同步，支持 OnChanged + OnRenamed
 * - 防回环机制：SyncingFiles + EventId + Hop(TTL)
 * - 文件下载、删除备份
 * - 关键步骤日志打印
 */

class SyncAgent
{
    // =========================
    // 配置对象
    // =========================
    public class Config
    {
        public string NodeName { get; set; } = "A";
        public int HttpPort { get; set; } = 5000;
        public Dictionary<string, string> Peers { get; set; } = new();
        public DirectoriesConfig Directories { get; set; } = new();
        public byte SyncingFilesValue { get; set; } = 1;
        public int DebounceDelayMs { get; set; } = 500;
    }

    public class DirectoriesConfig
    {
        public string RootDir { get; set; } = "odoo_filestore";
        public string DataDir { get; set; } = "data";
        public string DeletedBackupDir { get; set; } = "_deleted_backup";
    }

    public static Config Options = new();

    // =========================
    // 核心状态
    // =========================
    static readonly ConcurrentDictionary<string, FileMeta> Manifest = new();
    static readonly ConcurrentDictionary<string, byte> SyncingFiles = new();
    static readonly ConcurrentDictionary<string, DateTime> Debounce = new();
    static readonly ConcurrentDictionary<string, Task> PendingTasks = new();
    static readonly ConcurrentDictionary<string, byte> SeenEvents = new(); // EventId去重
    static FileSystemWatcher Watcher = null!;
    static readonly HttpListener Listener = new();

    static string RootDir => Path.Combine(AppContext.BaseDirectory, Options.Directories.RootDir);
    static string DataDir => Path.Combine(RootDir, Options.Directories.DataDir);
    static string DeletedBackupDir => Path.Combine(RootDir, Options.Directories.DeletedBackupDir);
    static string ManifestPath => Path.Combine(RootDir, "manifest.json");

    // =========================
    // 主入口
    // =========================
    public static async Task Main()
    {
        LoadConfig();

        Directory.CreateDirectory(DataDir);
        Directory.CreateDirectory(DeletedBackupDir);

        LoadManifest();
        StartWatcher();
        StartHttpServer();

        Console.WriteLine($"[{Options.NodeName}] SyncAgent running on port {Options.HttpPort}");
        await Task.Delay(-1);
    }

    // =========================
    // 配置读取
    // =========================
    static void LoadConfig()
    {
        const string cfgFile = "appsettings.json";
        if (!File.Exists(cfgFile))
        {
            Console.WriteLine("[WARN] appsettings.json not found, using defaults");
            return;
        }

        var json = File.ReadAllText(cfgFile);
        Options = JsonSerializer.Deserialize<Config>(json)!;
    }

    // =========================
    // 文件监听 + 防重复
    // =========================
    static void StartWatcher()
    {
        Watcher = new FileSystemWatcher(DataDir)
        {
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite,
            EnableRaisingEvents = true
        };

        Watcher.Created += OnChanged;
        Watcher.Changed += OnChanged;
        Watcher.Deleted += OnDeleted;
        Watcher.Renamed += OnRenamed;
    }
    static int ct=0;
    static void OnChanged(object sender, FileSystemEventArgs e)
    {
        ct++;
        Console.WriteLine(ct);
        return;

        Console.WriteLine($"*触发OnChanged：{e.FullPath}");
        if (Directory.Exists(e.FullPath)) return;
        if (SyncingFiles.ContainsKey(e.FullPath))
        {
            Console.WriteLine($"[SKIP] 文件正在下载，不通知：{e.FullPath}");
            return;
        }

        Debounce[e.FullPath] = DateTime.Now;

        if (PendingTasks.ContainsKey(e.FullPath)) return;

        var task = Task.Run(async () =>
        {
            try
            {
                await WaitFileStableSmart(e.FullPath, () =>
                {
                    var eventId = Guid.NewGuid().ToString();
                    Console.WriteLine($"[SYNC] 文件稳定，通知其他节点：{Path.GetFileName(e.FullPath)}");
                    Broadcast("update", Path.GetRelativePath(DataDir, e.FullPath).Replace("\\", "/"), eventId, 3);
                });
            }
            finally
            {
                PendingTasks.TryRemove(e.FullPath, out _);
            }
        });

        PendingTasks[e.FullPath] = task;
    }

    static void OnRenamed(object sender, RenamedEventArgs e)
    {
        if (SyncingFiles.ContainsKey(e.FullPath))
        {
            Console.WriteLine($"[SKIP] 文件正在下载，不通知重命名：{e.FullPath}");
            return;
        }

        Console.WriteLine($"[SYNC] 文件重命名 {Path.GetFileName(e.OldFullPath)} -> {Path.GetFileName(e.FullPath)}");

        var eventId = Guid.NewGuid().ToString();

        Broadcast("update", Path.GetRelativePath(DataDir, e.FullPath).Replace("\\", "/"), eventId, 3);
    }

    static void OnDeleted(object sender, FileSystemEventArgs e)
    {
        if (SyncingFiles.ContainsKey(e.FullPath))
        {
            Console.WriteLine($"[SKIP] 文件正在下载，不通知删除：{e.FullPath}");
            return;
        }

        Debounce[e.FullPath] = DateTime.Now;
        Task.Run(async () =>
        {
            await Task.Delay(Options.DebounceDelayMs);

            if (Debounce.TryGetValue(e.FullPath, out var lastEventTime))
            {
                if ((DateTime.Now - lastEventTime).TotalMilliseconds >= Options.DebounceDelayMs)
                {
                    Debounce.TryRemove(e.FullPath, out _);
                    Console.WriteLine($"[LOCAL DELETE] {e.FullPath}");
                    var eventId = Guid.NewGuid().ToString();
                    Broadcast("delete", Path.GetRelativePath(DataDir, e.FullPath).Replace("\\", "/"), eventId, 3);
                }
            }
        });
    }

    static async Task WaitFileStableSmart(string fullPath, Action onStable)
    {
        if (!File.Exists(fullPath)) return;

        var fi = new FileInfo(fullPath);
        long size = fi.Length;

        int stableCountRequired;
        int delayMs;

        if (size < 10 * 1024 * 1024) { stableCountRequired = 3; delayMs = 200; }
        else if (size < 100 * 1024 * 1024) { stableCountRequired = 5; delayMs = 500; }
        else if (size < 1024 * 1024 * 1024) { stableCountRequired = 10; delayMs = 500; }
        else { stableCountRequired = 20; delayMs = 1000; }

        long lastLength = -1;
        int stableCount = 0;

        while (true)
        {
            await Task.Delay(delayMs);

            if (!File.Exists(fullPath)) break;

            fi.Refresh();
            long len = fi.Length;

            if (len == lastLength)
            {
                stableCount++;
                if (stableCount >= stableCountRequired)
                {
                    onStable?.Invoke();
                    break;
                }
            }
            else
            {
                stableCount = 0;
                lastLength = len;
            }
        }
    }

    // =========================
    // HTTP Server
    // =========================
    static void StartHttpServer()
    {
        Listener.Prefixes.Add($"http://*:{Options.HttpPort}/");
        Listener.Start();

        Task.Run(async () =>
        {
            while (true)
            {
                var ctx = await Listener.GetContextAsync();
                _ = Task.Run(() => HandleHttp(ctx));
            }
        });
    }

    static async Task HandleHttp(HttpListenerContext ctx)
    {
        var req = ctx.Request;
        var res = ctx.Response;
        
        try
        {
            if (req.Url!.AbsolutePath == "/notify")
            {
                var body = await new StreamReader(req.InputStream).ReadToEndAsync();
                var msg = JsonSerializer.Deserialize<NotifyMsg>(body)!;
                HandleRemoteNotify(msg);
                Console.WriteLine($"[SYNC] 被其他节点请求更新本地文件: {req.Url}");
            }
            else if (req.Url.AbsolutePath == "/file")
            {
                var path = WebUtility.UrlDecode(req.QueryString["path"]);
                var full = Path.Combine(DataDir, path!.Replace("/", "\\"));
                if (!File.Exists(full)) { res.StatusCode = 404; return; }

                using var fs = File.OpenRead(full);
                res.ContentLength64 = fs.Length;
                await fs.CopyToAsync(res.OutputStream);

                Console.WriteLine($"[SYNC] 被其他节点请求下载文件: {path}");
            }
            else
            {
                res.StatusCode = 404;
            }
        }
        finally
        {
            res.Close();
        }
    }

    // =========================
    // 处理远程通知
    // =========================
    static void HandleRemoteNotify(NotifyMsg msg)
    {
        if (msg.Hop <= 0) return;
        if (SeenEvents.ContainsKey(msg.EventId)) return;
        SeenEvents[msg.EventId] = 1;

        var full = Path.Combine(DataDir, msg.Path.Replace("/", "\\"));

        if (msg.Type == "update")
        {
            SyncingFiles[full] = Options.SyncingFilesValue;

            _ = DownloadFile(msg.Source, msg.Path, full)
                .ContinueWith(_ =>
                {
                    Console.WriteLine("***********HandleRemoteNotify");
                    SyncingFiles.TryRemove(full, out byte _);
                   // Broadcast("update", msg.Path, msg.EventId, msg.Hop - 1);
                });
        }
        else if (msg.Type == "delete")
        {
            Console.WriteLine("***********HandleRemoteNotify-delete");
            BackupAndDelete(full);
           // Broadcast("delete", msg.Path, msg.EventId, msg.Hop - 1);
        }
    }

    static async Task DownloadFile(string sourceNode, string relativePath, string fullPath)
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            if (!Options.Peers.TryGetValue(sourceNode, out var peerUrl))
            {
                Console.WriteLine($"[WARN] Peer {sourceNode} not found");
                return;
            }

            var urlPath = WebUtility.UrlEncode(relativePath.Replace("\\", "/"));
            var url = $"{peerUrl}/file?path={urlPath}";

            Console.WriteLine($"[SYNC]从{sourceNode} 下载文件 {url} -> {fullPath}");
            using var wc = new WebClient();
            await wc.DownloadFileTaskAsync(url, fullPath);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] DownloadFile failed: {ex.Message}");
        }
    }

    static void BackupAndDelete(string full)
    {
        if (!File.Exists(full)) return;
        var backup = Path.Combine(
            DeletedBackupDir,
            Path.GetFileName(full) + "." + DateTime.Now.Ticks);
        File.Move(full, backup);
    }

    static void Broadcast(string type, string path, string eventId, int hop)
    {
        if (hop <= 0) return;

        var msg = new NotifyMsg
        {
            Source = Options.NodeName,
            Type = type,
            Path = path,
            EventId = eventId,
            Hop = hop
        };

        var json = JsonSerializer.Serialize(msg);
        var buf = Encoding.UTF8.GetBytes(json);

        foreach (var kv in Options.Peers)
        {
            var peerNode = kv.Key;
            var peerUrl = kv.Value;
            if (peerNode == Options.NodeName) continue;

            try
            {
                var req = (HttpWebRequest)WebRequest.Create($"{peerUrl}/notify");
                req.Method = "POST";
                req.ContentType = "application/json";
                req.ContentLength = buf.Length;
                req.Timeout = 5000;

                using (var stream = req.GetRequestStream())
                {
                    stream.Write(buf, 0, buf.Length);
                }
                using var resp = (HttpWebResponse)req.GetResponse();

                Console.WriteLine($"[SYNC] 发送通知Notify {peerNode} OK: {path} 来源：{msg.Source}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] Notify failed {peerNode}: {ex.Message}");
            }
        }
    }

    // =========================
    // Manifest
    // =========================
    static void LoadManifest()
    {
        if (!File.Exists(ManifestPath)) return;
        var json = File.ReadAllText(ManifestPath);
        var data = JsonSerializer.Deserialize<ConcurrentDictionary<string, FileMeta>>(json);
        if (data != null)
            foreach (var kv in data) Manifest[kv.Key] = kv.Value;
    }

    // =========================
    // 数据结构
    // =========================
    class NotifyMsg
    {
        public string Source { get; set; } = "";
        public string Type { get; set; } = "";
        public string Path { get; set; } = "";
        public string EventId { get; set; } = "";
        public int Hop { get; set; } = 3;
    }

    class FileMeta
    {
        public long Size { get; set; }
        public string Sha256 { get; set; } = "";
        public DateTime LastWriteUtc { get; set; }
    }
}
