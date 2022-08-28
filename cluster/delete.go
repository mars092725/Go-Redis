package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func Del(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 广播的执行模式要求所有数据库都需要执行成功才是成功
	replies := cluster.broadcase(c, cmdArgs)
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}
	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error : " + errReply.Error())
}
