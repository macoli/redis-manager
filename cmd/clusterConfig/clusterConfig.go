package clusterConfig

import (
	"fmt"

	"github.com/macoli/redis-manager/pkg/param"

	r "github.com/macoli/redis-manager/pkg/redis"
)

func Run() {
	addr, password, opType, config, setValue := param.ClusterConfig()

	//获取集群节点
	data, err := r.FormatClusterNodes(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败 err:%v\n", err)
		return
	}
	clusterNodes := append(data.Masters, data.Slaves...)
	//集群配置操作
	if opType == "get" {
		ret, err := r.ClusterGetConfig(clusterNodes, password, config)
		if err != nil {
			fmt.Printf("获取集群配置 %s 出错, err:%v\n", config, err)
			return
		}
		fmt.Printf("集群配置项%s的值为: %s\n", config, ret)
	} else if opType == "set" {
		if setValue == "" {
			fmt.Printf("集群 %s 的设置值为空\n", config)
			return
		}
		err := r.ClusterSetConfig(clusterNodes, password, config, setValue)
		if err != nil {
			fmt.Printf("设置集群配置 %s 失败, err:%v\n", config, err)
			return
		}
		fmt.Printf("设置集群配置 %s 成功\n", config)
	} else {
		fmt.Printf("类型参数配置错误\n")
		return
	}
}
