package iredis

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
)

var redisWG *sync.WaitGroup

// InitStandConn 初始化单例 iredis 连接
func InitStandConn(addr, password string) (*redis.Client, error) {
	rc := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
		PoolSize: 100,
	})

	_, err := rc.Ping(context.Background()).Result()
	if err != nil {
		errMsg := fmt.Sprintf("iredis 实例 %s 连接失败: %v\n", addr, err)
		return nil, errors.New(errMsg)
	}
	return rc, nil
}

// InitStandConnList 批量初始化多个 iredis 的连接
func InitStandConnList(addrSlice []string, password string) ([]*redis.Client, error) {
	var rcSlice []*redis.Client
	for _, addr := range addrSlice {
		rc := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       0,
			PoolSize: 100,
		})

		_, err := rc.Ping(context.Background()).Result()
		if err != nil {
			errMsg := fmt.Sprintf("批量建立 iredis 连接失败 : %v\n", err)
			return nil, errors.New(errMsg)
		}
		rcSlice = append(rcSlice, rc)
	}
	return rcSlice, nil
}

// InitSentinelMasterConn 初始化哨兵连接,通过哨兵获取到对应 master name 节点的 master 连接
func InitSentinelMasterConn(addrSlice []string, password, masterName string) (*redis.Client, error) {
	rc := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: addrSlice,
		Password:      password,
		PoolSize:      100,
	})

	_, err := rc.Ping(context.Background()).Result()
	if err != nil {
		errMsg := fmt.Sprintf("哨兵 %v 上 %s 的 master 连接失败: %v\n", addrSlice, masterName, err)
		return nil, errors.New(errMsg)
	}
	return rc, nil
}

// InitSentinelSlaveConn 初始化哨兵连接,通过哨兵获取到对应 master name 节点的 slave 只读连接
func InitSentinelSlaveConn(addrSlice []string, password, masterName string) (*redis.ClusterClient, error) {
	rc := redis.NewFailoverClusterClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: addrSlice,
		Password:      password,
		PoolSize:      100,
	})

	_, err := rc.Ping(context.Background()).Result()
	if err != nil {
		errMsg := fmt.Sprintf("哨兵 %v 上 %s 的 slave 连接失败: %v\n", addrSlice, masterName, err)
		return nil, errors.New(errMsg)
	}
	return rc, nil
}

// InitSentinelManagerConn 初始化哨兵管理连接,用于连接哨兵节点,管理哨兵
func InitSentinelManagerConn(addr, password string) (*redis.SentinelClient, error) {
	rc := redis.NewSentinelClient(&redis.Options{
		Addr:     addr,
		Password: password,
		PoolSize: 100,
	})

	_, err := rc.Ping(context.Background()).Result()
	if err != nil {
		errMsg := fmt.Sprintf("哨兵管理节点: %s 连接失败: %v\n", addr, err)
		return nil, errors.New(errMsg)
	}
	return rc, nil
}

// InitClusterConn 初始化集群连接
func InitClusterConn(addrSlice []string, password string) (*redis.ClusterClient, error) {
	rc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrSlice,
		Password: password,
		PoolSize: 100,
	})

	_, err := rc.Ping(context.Background()).Result()
	if err != nil {
		errMsg := fmt.Sprintf("集群节点: %s 连接失败: %v\n", addrSlice, err)
		return nil, errors.New(errMsg)
	}
	return rc, nil
}
