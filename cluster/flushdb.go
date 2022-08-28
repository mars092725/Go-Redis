package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func flushdb(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 广播的执行模式要求所有数据库都需要执行成功才是成功
	replies := cluster.broadcase(c, cmdArgs)
	var errReply reply.ErrorReply
	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return reply.MakeOkReply()
	}
	return reply.MakeErrReply("error : " + errReply.Error())
}
