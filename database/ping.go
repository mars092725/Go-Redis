package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// Ping the server
// ping不需要改变文件，所以不需要记录到aof
func Ping(db *DB, args [][]byte) resp.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func init() {
	RegisterCommand("ping", Ping, -1)
}
