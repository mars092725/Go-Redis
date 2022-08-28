package aof

import (
	"go-redis/config"
	"go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

type AofHandler struct {
	database    database.Database // 持有redis业务核心的数据库
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

// NewAofHandler  初始化一个aofhandler
func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	// 获取文件名
	handler.aofFilename = config.Properties.AppendFilename
	handler.database = database
	// load file  将以前写在硬盘中的文件恢复
	handler.LoadAof()
	// 打开文件
	aofile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofile

	// channel 用来数据库引擎在执行的时候将其放在硬盘中，是一个缓冲区
	handler.aofChan = make(chan *payload, aofQueueSize)
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// Add payload(set k v) -> aofchan  把即将执行的操作指令放在channel中（此时还没落盘）
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// handlerAof payload(set k v) <- aofChan
// 从channel里面取出操作指令，将其放入磁盘中
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
		}
	}
}

// LoadAof
// 启动aof的时候将上一次执行的指令重新执行一遍，相当于数据加载
func (handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFilename) // 简化版的openfile，该函数状态时只读状态
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()
	// file既是文件类型又是reader类型
	// parserstream返回一个管道
	ch := parser.ParseStream(file)
	fakeConn := &connection.FakeConn{}
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break // 读完文件了
			}
			logger.Error(p.Err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need muti mulk")
			continue
		}
		// 以上所有异常都处理过了，接下来处理管道中的数据
		rep := handler.database.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(rep) {
			logger.Error(rep)
		}
	}
}
