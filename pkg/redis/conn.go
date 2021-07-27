package redis

import (
	"context"

	"github.com/go-redis/redis/v8"
)

var (
	CTX = context.Background()
)

//InitStandRedis init standalone showSlowLog client
func InitStandRedis(addr string, password string) (rc *redis.Client, err error) {
	rc = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
		PoolSize: 100,
	})
	_, err = rc.Ping(CTX).Result()
	return
}

//InitClusterRedis init cluster showSlowLog client
func InitClusterRedis(addr string, password string) (rc *redis.ClusterClient, err error) {
	rc = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{addr},
		Password: password,
		PoolSize: 100,
	})

	_, err = rc.Ping(CTX).Result()
	return
}
