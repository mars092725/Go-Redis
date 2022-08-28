package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/config"
	database2 "go-redis/database"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strings"
)

// cluster其实是一个中介 连接客户端和每个独立的数据库
// 功能主要是将客户端发送的连接发送到适合的数据库中。
// 所以每个数据库中都有一个cluster，它和其他的数据库都需要建立连接，并且为了并发往往需要多个连接
// 如果共有三个节点（数据库），那么每个数据库的cluster就需要两个连接池
// 也就是说需要一个连接池，这里借用的是go-commons-pool，这时go中常用的连接池

type ClusterDatabase struct {
	self string

	nodes          []string
	peerPicker     *consistenthash.NodeMap
	Peerconnection map[string]*pool.ObjectPool // 连接池
	db             database.Database
}

func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self: config.Properties.Self,

		db:             database2.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		Peerconnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers))
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.Peerconnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply // 一个方法声明
var router = makeRouter()

func (c *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) { // 本地执行 转发执行 群发执行  三种执行模式
	// 返回值带名字，相当于在函数第一行定义了该变量，return的时候可以不加参数
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = &reply.UnknownErrReply{}

		}
	}()
	cmdName := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		reply.MakeErrReply("not supported cmd")
	}
	result = cmdFunc(c, client, args)
	return
}

func (c *ClusterDatabase) AfterClientClose(client resp.Connection) {
	c.db.AfterClientClose(client)
}

func (c *ClusterDatabase) Close() {
	c.db.Close()
}
