package database

import (
	"fmt"
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

// Database is a set of multiple database set
type StandaloneDatabase struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler
}

// NewDatabase creates a redis database,
func NewStandaloneDatabase() *StandaloneDatabase {
	mdb := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(mdb)
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			// 循环16个db
			sdb := db
			db.addAof = func(line CmdLine) {
				// 项目问题有了  ^_^
				// 因为mdb这时引用了外部函数addAof，此时该函数使用的局部变量db逃逸到了堆上
				// 所以说等到遍历到了第十五个的时候，此时在堆上的变量db也变成了15
				// 而之前遍历的函数中调用addaof传入的db.index都变成了15
				// 所以重新定义了一个变量sdb
				mdb.aofHandler.AddAof(sdb.index, line)
			}
		}
	}
	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (mdb *StandaloneDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// Close graceful shutdown database
func (mdb *StandaloneDatabase) Close() {

}

func (mdb *StandaloneDatabase) AfterClientClose(c resp.Connection) {
}

func execSelect(c resp.Connection, mdb *StandaloneDatabase, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
