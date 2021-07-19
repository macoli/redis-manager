package showSlowLog

import (
	"context"

	"github.com/go-redis/redis/v8"
)

var (
	CTX = context.Background()
)

//InitStandRedis init standalone redis client
func InitStandRedis(addr string) (rc *redis.Client, err error) {
	rc = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
		PoolSize: 100,
	})
	_, err = rc.Ping(CTX).Result()
	return
}

//InitClusterRedis init cluster redis client
func InitClusterRedis(addr string) (rc *redis.ClusterClient, err error) {
	rc = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{addr},
	})

	_, err = rc.Ping(CTX).Result()
	return
}

//GetClusterInstances get all redis instance from cluster redis
func GetClusterNodes(addr string) (ret string, err error) {
	// init redis cluster conn
	var rc *redis.ClusterClient
	rc, err = InitClusterRedis(addr)
	defer rc.Close()

	// redis command: cluster nodes
	ret, err = rc.ClusterNodes(CTX).Result()
	if err != nil {
		return "", err
	}
	return
}

// GetSlowLog get slow log info
func GetSlowLog(addr string) (ret []redis.SlowLog, err error) {
	// init redis conn
	rc, err := InitStandRedis(addr)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// get slow log numbers
	nums, err := rc.Do(CTX, "slowlog", "len").Result()
	if err != nil {
		return nil, err
	}

	// get slow log info
	ret, err = rc.SlowLogGet(CTX, nums.(int64)).Result()
	if err != nil {
		return nil, err
	}
	return
}
