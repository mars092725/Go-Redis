package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// rename指令会更改key值，有可能更改过的kv对就不在原本的节点了
// 实现的功能是：如果发现更改过的key不能放在该节点中就报错

func rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.MakeErrReply("Err Wrong number args")
	}
	src := string(cmdArgs[1])
	dest := string(cmdArgs[2])

	srcpeer := cluster.peerPicker.PickNode(src)
	destpeer := cluster.peerPicker.PickNode(dest)
	if srcpeer != destpeer {
		return reply.MakeErrReply("ERR rename must within one peer")
	}
	return cluster.relay(srcpeer, c, cmdArgs)
}
