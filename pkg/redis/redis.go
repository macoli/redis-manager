package showSlowLog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	CTX = context.Background()
)

//InitStandRedis init standalone redis client
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

//InitClusterRedis init cluster redis client
func InitClusterRedis(addr string, password string) (rc *redis.ClusterClient, err error) {
	rc = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{addr},
		Password: password,
		PoolSize: 100,
	})

	_, err = rc.Ping(CTX).Result()
	return
}

//GetClusterInstances get all redis instance from cluster redis
func GetClusterNodes(addr string, password string) (ret string, err error) {
	// init redis cluster conn
	var rc *redis.ClusterClient
	rc, err = InitClusterRedis(addr, password)
	defer rc.Close()

	// redis command: cluster nodes
	ret, err = rc.ClusterNodes(CTX).Result()
	if err != nil {
		return "", err
	}
	return
}

// GetSlowLog get slow log info
func GetSlowLog(addr string, password string) (ret []redis.SlowLog, err error) {
	// init redis conn
	rc, err := InitStandRedis(addr, password)
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

//SetSlotImporting 对目标节点执行 cluster setslot [slot] importing [source nodeID]
func SetSlotImporting(targetAddr string, password string, slot int, sourceNodeID string) (err error) {
	rc, err := InitStandRedis(targetAddr, password)
	if err != nil {
		return
	}
	defer rc.Close()
	_, err = rc.Do(CTX, "cluster", "setslot", slot, "importing", sourceNodeID).Result()
	if err != nil {
		return
	}
	return nil
}

//SetSlotMigrating 对源节点发送 cluster setslot [slot] migrating [target nodeID]
func SetSlotMigrating(sourceAddr string, password string, slot int, targetNodeID string) (err error) {
	rc, err := InitStandRedis(sourceAddr, password)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = rc.Do(CTX, "cluster", "setslot", slot, "migrating", targetNodeID).Result()
	if err != nil {
		return err
	}
	return nil
}

//MoveData 迁移 slot 数据
func MoveData(sourceAddr string, targetAddr string, password string, slot int, count int) (err error) {
	//连接源节点
	rc, err := InitStandRedis(sourceAddr, password)
	if err != nil {
		return err
	}
	defer rc.Close()

	//获取目标节点的 ip 和 port
	targetIP := strings.Split(targetAddr, ":")[0]
	targetPort := strings.Split(targetAddr, ":")[1]

	//循环迁移 slot 的数据到目标节点
	for {
		ret := rc.ClusterGetKeysInSlot(CTX, slot, count)
		//fmt.Printf("%#v\n", ret)
		for _, key := range ret.Val() {
			_, err := rc.Migrate(CTX, targetIP, targetPort, key, 0, time.Second*10).Result()
			if err != nil {
				return err
			}
			//fmt.Printf("migrate result:%v\n", ret)
			fmt.Printf("#")
		}
		if len(ret.Val()) < count {
			fmt.Printf("\n")
			break
		}
	}
	return nil
}

//SetSlotNode 向集群内所有主节点发送 cluster setslot [slot] node [target nodeID],以通知 slot 已经分配给了目标节点
func SetSlotNode(clusterNodes []string, password string, slot int, targetNodeID string) (err error) {
	for _, addr := range clusterNodes {
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}

		_, err = rc.Do(CTX, "cluster", "setslot", slot, "node", targetNodeID).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

//ClusterGetConfig 获取集群配置并校验是否一致
func ClusterGetConfig(addrSlice []string, password string, configArg string) (ret string, err error) {
	for _, addr := range addrSlice {
		// init redis conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return "", err
		}

		argRet, err := rc.ConfigGet(CTX, configArg).Result()
		if err != nil {
			return "", err
		}
		retValue := argRet[1].(string)
		if ret != argRet[1].(string) && ret != "" {
			err = errors.New("集群配置项的值不一致")
			return "", err
		} else {
			ret = retValue
		}
		rc.Close()
	}
	return
}

//ClusterSetConfig 批量设置集群配置
func ClusterSetConfig(addrSlice []string, password string, configArg string, setValue string) (err error) {
	for _, addr := range addrSlice {
		// init redis conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}

		err = rc.ConfigSet(CTX, configArg, setValue).Err()
		if err != nil {
			return err
		}
		rc.Close()
	}
	return
}

//ClusterFlushAll 集群数据清理
func ClusterFlushAll(ClusterNodesSlice []string, password string, flushCMD string) (err error) {
	for _, addr := range ClusterNodesSlice {
		// init redis conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}
		defer rc.Close()

		//获取集群所有节点参数 cluster-node-timeout 值
		ret, err := rc.ConfigGet(CTX, "cluster-node-timeout").Result()
		if err != nil {
			return err
		}
		clusterTimeoutValue := ret[1].(string)

		//重设参数 cluster-node-timeout 值,防止在执行 FLUSHALL 命令时发生主从切换(10分钟)
		err = rc.ConfigSet(CTX, "cluster-node-timeout", "600000").Err()
		if err != nil {
			return err
		}

		//对每个节点执行 FLUSHALL 命令
		if flushCMD == "FLUSHALL" {
			err = rc.FlushAll(CTX).Err()
			if err != nil {
				return err
			}
		} else {
			err = rc.Do(CTX, flushCMD).Err()
			return err
		}

		//将参数 cluster-node-timeout 还原
		err = rc.ConfigSet(CTX, "cluster-node-timeout", clusterTimeoutValue).Err()
		if err != nil {
			return err
		}
	}
	return
}
