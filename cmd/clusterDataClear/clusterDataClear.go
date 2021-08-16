package clusterDataClear

import (
	"flag"
	"fmt"

	"github.com/macoli/redis-manager/cmd/paramDeal"

	"github.com/macoli/redis-manager/pkg/redis"
)

func Param() (string, string, string) {
	clusterDataClear := flag.NewFlagSet("clusterclear", flag.ExitOnError)
	addr := clusterDataClear.String("addr", "127.0.0.1:6379", "redis地址")
	password := clusterDataClear.String("password", "", "redis密码")
	flushCMD := clusterDataClear.String("flushcmd", "FLUSHALL",
		"清空命令,当清空命令被重命名后使用")
	paramDeal.ParamsCheck(clusterDataClear)

	return *addr, *password, *flushCMD
}

func Run() {
	addr, password, flushCMD := Param()

	// 获取集群信息
	data, err := redis.FormatClusterNodes(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败, err:%v\n", err)
		return
	}

	// 获取cluster-node-timeout配置值
	clusterNodes := append(data.Masters, data.Slaves...)
	ret, err := redis.ClusterGetConfig(clusterNodes, password, "cluster-node-timeout")
	if err != nil {
		fmt.Printf("获取集群配置项cluster-node-timeout失败, err:%v\n", err)
		return
	}
	// 清空集群所有节点
	err = redis.ClusterFlushAll(data, password, flushCMD)
	if err != nil {
		fmt.Printf("清空集群节点失败 err:%v\n", err)
		_ = redis.ClusterSetConfig(data.Masters, password, "cluster-node-timeout", ret)
		return
	}

	// 将cluster-node-timeout配置修改为原来配置的值
	err = redis.ClusterSetConfig(clusterNodes, password, "cluster-node-timeout", ret)
	if err != nil {
		fmt.Printf("还原集群配置项cluster-node-timeout失败,配置项初始值为%s, err:%v\n", ret, err)
		return
	}

	fmt.Printf("集群已清空\n")
}
