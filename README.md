# syncthing.dotnet
* - 仿制SYNCTHING的功能
* - 支持多服务器P2P的文件同步
* - 同步速度比syncthing快
* - 在windows上面没有文件数量限制，linux下面只能监测8192个变化点，超了无法感知
* - 多节点同步，支持 OnChanged + OnRenamed
* - 防回环机制：SyncingFiles + EventId + Hop(TTL)
* - 文件下载、删除备份
* - 关键步骤日志打印
