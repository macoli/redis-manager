package showClusterMap

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/macoli/redis-manager/cmd/paramDeal"

	t "github.com/macoli/redis-manager/pkg/table"

	r "github.com/macoli/redis-manager/pkg/redis"
)

// data sort
func dataSort(s string, data []*r.ClusterNode) {
	sort.Slice(data, func(i, j int) bool {
		switch s {
		case "MasterID":
			return data[i].MasterID < data[j].MasterID
		case "MasterAddr":
			return data[i].MasterAddr < data[j].MasterAddr
		case "SlaveAddr":
			return data[i].SlaveAddr < data[j].SlaveAddr
		case "SlaveID":
			return data[i].SlaveID < data[j].SlaveID
		default:
			return data[i].MasterAddr < data[j].MasterAddr
		}

	})
}

//show the cluster map by table
func show(data []*r.ClusterNode, sortBy string) {
	if len(data) == 0 {
		fmt.Println("集群信息为空,请检查集群状态")
		os.Exit(0)
	}
	dataSort(sortBy, data)

	HeaderCells := t.GenHeaderCells(r.ClusterNode{})

	dataInterface := make([]interface{}, len(data))
	for i, rowMap := range data {
		row := []string{
			rowMap.MasterID,
			rowMap.MasterAddr,
			rowMap.SlaveAddr,
			rowMap.SlaveID,
		}
		dataInterface[i] = row
	}
	BodyCells := t.GenBodyCells(dataInterface)

	t.ShowTable(HeaderCells, BodyCells)
}

func Param() (string, string, string) {
	clusterMap := flag.NewFlagSet("clustermap", flag.ExitOnError)
	addr := clusterMap.String("addr", "127.0.0.1:6379", "redis地址")
	password := clusterMap.String("pass", "", "redis密码")
	sortBy := clusterMap.String("sortby", "MasterAddr",
		"按不通列排序:MasterID/MasterAddr/slaveAddr/slaveID")
	paramDeal.ParamsCheck(clusterMap)
	return *addr, *password, *sortBy
}

// Run show cluster map main
func Run() {
	addr, password, sortBy := Param()

	data, err := r.FormatClusterNodes(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败, err:%v\n", err)
	}
	show(data.ClusterNodesDetail, sortBy)
}
