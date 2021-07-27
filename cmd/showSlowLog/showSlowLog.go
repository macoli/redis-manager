package showSlowLog

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	r "github.com/macoli/redis-manager/pkg/redis"
	t "github.com/macoli/redis-manager/pkg/table"
)

// SlowLog type
type SlowLog struct {
	Instance string
	Command  string
	Duration time.Duration
	Time     string
}

//get slow log and format
func formatSlowLog(addr string, password string) ([]SlowLog, error) {
	//get slow log
	ret, err := r.GetSlowLog(addr, password)
	if err != nil {
		return nil, err
	}
	// format slow log
	var data []SlowLog
	for _, item := range ret {
		tmp := SlowLog{
			addr,
			strings.Join(item.Args, " "),
			item.Duration,
			item.Time.String(),
		}
		data = append(data, tmp)
	}
	return data, err
}

//get cluster nodes and format
func formatClusterNodes(addr string, password string) (instances []string, err error) {
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
		instance := strings.Split(nodeSlice[1], "@")[0]
		instances = append(instances, instance)
	}
	return
}

// data sort
func dataSort(s string, data []SlowLog) {
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

// show table
func show(data []SlowLog, sortColumn string) {
	if len(data) == 0 {
		fmt.Println("当前redis没有慢查询信息")
		os.Exit(0)
	}
	dataSort(sortColumn, data)

	HeaderCells := t.GenHeaderCells(SlowLog{})

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
	BodyCells := t.GenBodyCells(dataInterface)

	t.ShowTable(HeaderCells, BodyCells)
}

//Run show slow log main func
func Run(instance, password, redisType, sortBy string) {
	// get all slow log info
	var slowLogs []SlowLog
	switch {
	case redisType == "standalone": //standalone type
		ret, err := formatSlowLog(instance, password)
		if err != nil {
			fmt.Printf("获取慢查询失败, err:%v\n", err)
			return
		}
		slowLogs = append(slowLogs, ret...)
	case redisType == "cluster":
		//get all instance list from cluster
		instances, err := formatClusterNodes(instance, password)
		if err != nil {
			fmt.Printf("获取集群节点信息失败, err:%v\n", err)
			return
		}

		for _, instance := range instances { //cluster type
			ret, err := formatSlowLog(instance, password)
			if err != nil {
				fmt.Printf("获取慢查询失败, err:%v\n", err)
				return
			}
			slowLogs = append(slowLogs, ret...)
		}
	}

	// show the slow log info by table
	show(slowLogs, sortBy)
}
