package cluster

import (
	"context"
	"errors"
	"go-redis/client"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
	"strconv"
)

// 负责节点之间的通信

func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	// peer:兄弟结点的地址，找到该节点的连接池
	pool, ok := cluster.Peerconnection[peer]
	if !ok {
		return nil, errors.New("connection not fount")
	}
	// 借一个连接出来
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wront type")
	}
	return c, err
}

func (cluster *ClusterDatabase) returnPeerClient(peer string, client *client.Client) error {
	// 把连接池中的连接进行归还
	pool, ok := cluster.Peerconnection[peer]
	if !ok {
		return errors.New("connection not fount")
	}
	return pool.ReturnObject(context.Background(), client)
}

func (cluster ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	// 转发模式
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

func (cluster ClusterDatabase) broadcase(c resp.Connection, args [][]byte) map[string]resp.Reply {
	// 广播发送
	results := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args)
		results[node] = result
	}
	return results
}
