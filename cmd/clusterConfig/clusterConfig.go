package clusterConfig

import (
	"flag"
	"fmt"

	"github.com/macoli/redis-manager/cmd/paramDeal"

	r "github.com/macoli/redis-manager/pkg/redis"
)

// param 获取参数
func param() (string, string, string, string, string) {
	clusterConfig := flag.NewFlagSet("clusterconfig", flag.ExitOnError)
	addr := clusterConfig.String("addr", "127.0.0.1:6379", "redis地址")
	password := clusterConfig.String("password", "", "redis集群密码")
	opType := clusterConfig.String("type", "get", "操作的类型,可选项:set/get")
	config := clusterConfig.String("config", "", "操作的配置项")
	setValue := clusterConfig.String("value", "", "设置集群配置项时,配置项的值.仅当吵着类型是set时生效")
	paramDeal.ParamsCheck(clusterConfig)

	return *addr, *password, *opType, *config, *setValue
}

func Run() {
	addr, password, opType, config, setValue := param()

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
