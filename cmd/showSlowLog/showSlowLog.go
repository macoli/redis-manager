package showSlowLog

import (
	"fmt"
	"os"
	"sort"

	"github.com/macoli/redis-manager/pkg/param"

	"github.com/macoli/redis-manager/pkg/redis"
	"github.com/macoli/redis-manager/pkg/table"
)

// dataSort 排序
func dataSort(s string, data []redis.SlowLog) {
	sort.Slice(data, func(i, j int) bool {
		switch s {
		case "Instance":
			return data[i].Instance < data[j].Instance
		case "Command":
			return data[i].Command < data[j].Command
		case "Duration":
			return data[i].Duration > data[j].Duration
		case "Time":
			return data[i].Time > data[j].Time
		default:
			return data[i].Time > data[j].Time
		}

	})
}

// show  通过表格展示
func show(data []redis.SlowLog, sortColumn string) {
	if len(data) == 0 {
		fmt.Println("当前redis没有慢查询信息")
		os.Exit(0)
	}
	dataSort(sortColumn, data)

	HeaderCells := table.GenHeaderCells(redis.SlowLog{})

	dataInterface := make([]interface{}, len(data))
	for i, rowMap := range data {
		row := []string{
			rowMap.Instance,
			rowMap.Command,
			rowMap.Duration.String(),
			rowMap.Time,
		}
		dataInterface[i] = row
	}
	BodyCells := table.GenBodyCells(dataInterface)

	table.ShowTable(HeaderCells, BodyCells)
}

//Run 获取 redis 慢查询
func Run() {
	addr, password, redisType, sortBy := param.SlowLog()
	// 获取 redis 实例的慢查询日志
	var slowLogs []redis.SlowLog
	switch {
	case redisType == "standalone": // 单实例 redis 类型
		ret, err := redis.FormatSlowLog(addr, password)
		if err != nil {
			fmt.Printf("获取慢查询失败, err:%v\n", err)
			return
		}
		slowLogs = append(slowLogs, ret...)
	case redisType == "cluster": // 集群 redis 类型
		// 获取集群所有节点信息
		data, err := redis.FormatClusterNodes(addr, password)
		if err != nil {
			fmt.Printf("获取集群节点信息失败, err:%v\n", err)
			return
		}
		clusterNodes := append(data.Masters, data.Slaves...) // 集群所有实例
		// 循环获取集群所有节点的慢查询日志
		for _, instance := range clusterNodes {
			ret, err := redis.FormatSlowLog(instance, password)
			if err != nil {
				fmt.Printf("获取集群节点 %s 的慢查询日志失败, err:%v\n", instance, err)
				return
			}
			slowLogs = append(slowLogs, ret...)
		}
	}

	// 通过表格展示慢查询日志
	show(slowLogs, sortBy)
}
