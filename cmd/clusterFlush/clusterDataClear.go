package clusterFlush

import (
	"fmt"

	"github.com/macoli/redis-manager/pkg/param"

	"github.com/macoli/redis-manager/pkg/redis"
)

func Run() {
	addr, password, flushCMD := param.ClusterFlush()

	// 获取集群信息
	data, err := redis.FormatClusterNodes(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败, err:%v\n", err)
		return
	}

	// 清空集群所有节点
	err = redis.ClusterFlushAll(data, password, flushCMD)
	if err != nil {
		fmt.Printf("清空集群节点失败 err:%v\n", err)
		return
	}
}
