package redis

import (
	"time"

	"github.com/go-redis/redis/v8"
)

//InitStandRedis 初始化单例 redis 连接
func InitStandRedis(addr string, password string) (rc *redis.Client, err error) {
	rc = redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DB:          0,
		PoolSize:    100,
		DialTimeout: time.Minute * 30,
	})
	_, err = rc.Ping(ctx).Result()
	return
}

//InitSentinelRedis 初始化哨兵连接
func InitSentinelRedis(addrSlice []string, password string, masterName string) (rc *redis.Client, err error) {
	rc = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: addrSlice,
		Password:      password,
		PoolSize:      1000,
		DialTimeout:   time.Minute * 30,
	})
	_, err = rc.Ping(ctx).Result()
	return
}

//InitClusterRedis 初始化集群连接
func InitClusterRedis(addrSlice []string, password string) (rc *redis.ClusterClient, err error) {
	rc = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrSlice,
		Password: password,
		PoolSize: 1000,
	})

	_, err = rc.Ping(ctx).Result()
	return
}
