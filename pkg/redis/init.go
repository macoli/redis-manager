package redis

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

//InitStandRedis 初始化单例 redis 连接
func InitStandRedis(addr string, password string) (*redis.Client, error) {
	rc := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DB:          0,
		PoolSize:    100,
		DialTimeout: time.Minute * 30,
	})
	_, err := rc.Ping(ctx).Result()
	if err != nil {
		errMsg := fmt.Sprintf("连接 redis 实例: %s 失败, err:%v\n", addr, err)
		return nil, errors.New(errMsg)
	}
	return rc, err
}

//InitSentinelRedis 初始化哨兵连接,通过哨兵获取到对应 master name 节点的连接
func InitSentinelRedis(addrSlice []string, password string, masterName string) (*redis.Client, error) {
	rc := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: addrSlice,
		Password:      password,
		PoolSize:      1000,
		DialTimeout:   time.Minute * 30,
	})
	_, err := rc.Ping(ctx).Result()
	if err != nil {
		errMsg := fmt.Sprintf("连接哨兵 %v 对应的 master name 节点: %s 失败, err:%v\n", addrSlice, masterName, err)
		return nil, errors.New(errMsg)
	}
	return rc, err
}

//InitSentinelManageRedis 初始化哨兵管理连接,用于连接哨兵节点,管理哨兵
func InitSentinelManageRedis(addr string, password string) (*redis.SentinelClient, error) {
	rc := redis.NewSentinelClient(&redis.Options{
		Addr:     addr,
		Password: password,
		PoolSize: 100,
	})

	_, err := rc.Ping(ctx).Result()
	if err != nil {
		errMsg := fmt.Sprintf("连接哨兵管理节点: %s 失败, err:%v\n", addr, err)
		return nil, errors.New(errMsg)
	}
	return rc, err
}

//InitClusterRedis 初始化集群连接
func InitClusterRedis(addrSlice []string, password string) (*redis.ClusterClient, error) {
	rc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrSlice,
		Password: password,
		PoolSize: 1000,
	})

	_, err := rc.Ping(ctx).Result()
	if err != nil {
		errMsg := fmt.Sprintf("连接集群: %s 失败, err:%v\n", addrSlice, err)
		return nil, errors.New(errMsg)
	}
	return rc, err
}
