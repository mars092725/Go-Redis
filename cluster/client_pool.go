package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/client"
)

type connectionFactory struct {
	Peer string // 连接的是哪个IP（位置）
}

func (f connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	return pool.NewPooledObject(c), nil
}

func (f connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client) // 线程池转化为客户端
	if !ok {
		return errors.New("type mismach")
	}
	c.Close() // 关闭客户端
	return nil
}

func (f connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (f connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (f connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
