package showSlowLog

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/macoli/redis-manager/cmd/paramDeal"

	r "github.com/macoli/redis-manager/pkg/redis"
	t "github.com/macoli/redis-manager/pkg/table"
)

// data sort
func dataSort(s string, data []r.SlowLog) {
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
func show(data []r.SlowLog, sortColumn string) {
	if len(data) == 0 {
		fmt.Println("当前redis没有慢查询信息")
		os.Exit(0)
	}
	dataSort(sortColumn, data)

	HeaderCells := t.GenHeaderCells(r.SlowLog{})

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

func Param() (string, string, string, string) {
	slowLog := flag.NewFlagSet("slowlog", flag.ExitOnError)
	addr := slowLog.String("addr", "127.0.0.1:6379", "redis地址")
	password := slowLog.String("pass", "", "redis密码")
	redisType := slowLog.String("type", "standalone", "redis类型选择:standalone/cluster")
	sortBy := slowLog.String("sortby", "Time", "按不通列排序:Instance/Command/Duration/Time")
	paramDeal.ParamsCheck(slowLog)
	return *addr, *password, *redisType, *sortBy
}

//Run show slow log main func
func Run() {
	addr, password, redisType, sortBy := Param()
	// get all slow log info
	var slowLogs []r.SlowLog
	switch {
	case redisType == "standalone": //standalone type
		ret, err := r.FormatSlowLog(addr, password)
		if err != nil {
			fmt.Printf("获取慢查询失败, err:%v\n", err)
			return
		}
		slowLogs = append(slowLogs, ret...)
	case redisType == "cluster":
		//get all instance list from cluster
		data, err := r.FormatClusterNodes(addr, password)
		if err != nil {
			fmt.Printf("获取集群节点信息失败, err:%v\n", err)
			return
		}
		clusterNodes := append(data.Masters, data.Slaves...)
		for _, instance := range clusterNodes { //cluster type
			ret, err := r.FormatSlowLog(instance, password)
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
