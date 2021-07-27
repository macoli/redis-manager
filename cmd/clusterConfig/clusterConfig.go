package clusterConfig

import (
	"errors"
	"fmt"
	"strings"

	r "github.com/macoli/redis-manager/pkg/redis"
)

//get cluster nodes and format
func getClusterNodes(addr string, password string) (clusterNodeMap map[string][]string, err error) {
	clusterNodeMap = make(map[string][]string)

	//get cluster nodes
	ret, err := r.GetClusterNodes(addr, password)
	if err != nil {
		return nil, err
	}
	// format the ret, get all instance addr
	clusterNodesSlice := strings.Split(ret, "\n")
	for _, node := range clusterNodesSlice {
		if len(node) == 0 {
			continue
		}
		nodeSlice := strings.Split(node, " ")

		role := nodeSlice[2]
		if strings.Contains(role, "myself") {
			role = strings.Split(role, ",")[1]
		}

		if role == "master" {
			masterAddr := strings.Split(nodeSlice[1], "@")[0]
			clusterNodeMap["master"] = append(clusterNodeMap["master"], masterAddr)
		} else if role == "slave" {
			slaveAddr := strings.Split(nodeSlice[1], "@")[0]
			clusterNodeMap["slave"] = append(clusterNodeMap["slave"], slaveAddr)
		} else {
			msg := fmt.Sprintf("cluster node error: %s", nodeSlice)
			err = errors.New(msg)
			return
		}
	}
	return
}

func Run(addr string, password string, opType string, config string, setValue string) {
	//获取集群节点
	clusterNodeMap, err := getClusterNodes(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败 err:%v\n", err)
		return
	}
	clusterNodeSlice := append(clusterNodeMap["master"], clusterNodeMap["slave"]...)
	//集群配置操作
	if opType == "get" {
		ret, err := r.ClusterGetConfig(clusterNodeSlice, password, config)
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
		err := r.ClusterSetConfig(clusterNodeSlice, password, config, setValue)
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
