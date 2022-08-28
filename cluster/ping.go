package cluster

import "go-redis/interface/resp"

// ping指令不是转发模式的指令，本地执行即可

func ping(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdArgs)
}
