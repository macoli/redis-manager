package clustermap

import (
	"fmt"
	"os"
	"sort"

	"github.com/macoli/redis-manager/pkg/param"

	"github.com/macoli/redis-manager/pkg/itable"

	"github.com/macoli/redis-manager/pkg/iredis"
)

// dataSort 排序
func dataSort(s string, data []*iredis.MasterSlaveMap) {
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

//show 通过表格展示
func show(data []*iredis.MasterSlaveMap, sortBy string) {
	if len(data) == 0 {
		fmt.Println("集群信息为空,请检查集群状态")
		os.Exit(0)
	}
	dataSort(sortBy, data)

	HeaderCells := itable.GenHeaderCells(iredis.MasterSlaveMap{})

	dataInterface := make([]interface{}, len(data))
	for i, rowMap := range data {
		row := []string{
			rowMap.MasterID,
			rowMap.MasterAddr,
			rowMap.SlaveAddr,
			rowMap.SlaveID,
			rowMap.SlotStr,
		}
		dataInterface[i] = row
	}
	BodyCells := itable.GenBodyCells(dataInterface)

	itable.ShowTable(HeaderCells, BodyCells)
}

// Run 获取集群关系并通过表格展示
func Run() {
	addr, password, sortBy := param.ClusterMap()

	data, err := iredis.ClusterInfoFormat(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败, err:%v\n", err)
		return
	}
	//for _, d := range data.MasterSlaveMaps {
	//	fmt.Printf("%#v\n", *d)
	//}
	show(data.MasterSlaveMaps, sortBy)
}
