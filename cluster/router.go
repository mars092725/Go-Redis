package cluster

import "go-redis/interface/resp"

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["set"] = defaultFunc
	routerMap["ssetnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["ping"] = ping
	routerMap["rename"] = rename
	routerMap["renamenx"] = rename // 在集群层我们只关注将指令发送到哪一个数据库中，只关注转发操作
	routerMap["flushdb"] = flushdb
	routerMap["deleted"] = Del
	routerMap["select"] = execSelect // 因为我们在每次转发一个命令的时候，都会现转发一个select命令，所以当用户真的发送select时，只需要本地执行

	return routerMap
}

func defaultFunc(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 我们发现大多数指令都是转发给某一个节点
	key := string(cmdArgs[1])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, cmdArgs)
}
