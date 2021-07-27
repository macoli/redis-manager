package clusterDataClear

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

func Run(addr string, password string, flushCMD string) {
	//获取集群所有节点
	clusterNodeMap, err := getClusterNodes(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败, err:%v\n", err)
		return
	}

	//获取cluster-node-timeout配置值
	clusterSlice := append(clusterNodeMap["master"], clusterNodeMap["slave"]...)
	ret, err := r.ClusterGetConfig(clusterSlice, password, "cluster-node-timeout")
	if err != nil {
		fmt.Printf("获取集群配置项cluster-node-timeout失败, err:%v\n", err)
		return
	}
	//修改cluster-node-timeout值为 10min,避免执行 FLUSHALL 命令时由于超时发生主从切换
	err = r.ClusterFlushAll(clusterNodeMap["master"], password, flushCMD)
	if err != nil {
		fmt.Printf("清空集群节点失败 err:%v\n", err)
		return
	}

	//将cluster-node-timeout配置修改为原来配置的值
	err = r.ClusterSetConfig(clusterSlice, password, "cluster-node-timeout", ret)
	if err != nil {
		fmt.Printf("还原集群配置项cluster-node-timeout失败,配置项初始值为%s, err:%v\n", ret, err)
		return
	}

	fmt.Printf("集群已清空\n")
}
