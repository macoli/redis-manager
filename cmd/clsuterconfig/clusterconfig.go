package clsuterconfig

import (
	"fmt"

	"github.com/macoli/redis-manager/pkg/param"

	"github.com/macoli/redis-manager/pkg/redis"
)

func Run() {
	addr, password, opType, config, setValue := param.ClusterConfig()

	//获取集群节点
	data, err := redis.ClusterInfoFormat(addr, password)
	if err != nil {
		fmt.Printf("集群节点信息获取失败 err:%v\n", err)
		return
	}
	clusterNodes := append(data.Masters, data.Slaves...)
	//集群配置操作
	if opType == "get" {
		ret, err := redis.ClusterConfigGet(clusterNodes, password, config)
		if err != nil {
			fmt.Printf("err: 集群配置项 %s 的值获取失败: %v\n", config, err)
			return
		}
		fmt.Printf("集群配置项 %s 的值为: %s\n", config, ret)
	} else if opType == "set" {
		if setValue == "" {
			fmt.Printf("err: 集群配置项 %s 的设置值为空\n", config)
			return
		}
		err := redis.ClusterConfigSet(clusterNodes, password, config, setValue)
		if err != nil {
			fmt.Printf("err: 集群配置项 %s 设置值 %s 失败: %v\n", config, setValue, err)
			return
		}
		fmt.Printf("集群配置项 %s 设置值 %s 成功\n", config, setValue)
	} else {
		fmt.Printf("类型参数请指定为: get/set\n")
		return
	}
}
