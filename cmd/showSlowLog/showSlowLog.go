package showSlowLog

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alexeyco/simpletable"

	"github.com/go-redis/redis/v8"
)

var (
	ctx = context.Background()
)

// init standalone redis client
func initStandRedis(addr string) (rc *redis.Client, err error) {
	rc = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
		PoolSize: 100,
	})
	_, err = rc.Ping(ctx).Result()
	return
}

// init cluster redis client
func initClusterRedis(addr string) (rc *redis.ClusterClient, err error) {
	rc = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{addr},
	})

	_, err = rc.Ping(ctx).Result()
	return
}

// get all redis instance from cluster redis
func getAllInstance(addr string) (instances []string, err error) {
	// init redis cluster conn
	var rc *redis.ClusterClient
	rc, err = initClusterRedis(addr)
	defer rc.Close()

	// redis command: cluster nodes
	var ret string
	ret, err = rc.ClusterNodes(ctx).Result()

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

// SlowLog type
type SlowLog struct {
	Instance string
	Command  string
	Duration time.Duration
	Time     string
}

func getSlowLog(addr string) (data []SlowLog, err error) {

	// init redis conn
	var rc *redis.Client
	rc, err = initStandRedis(addr)
	defer rc.Close()

	// get slow log numbers
	nums, err := rc.Do(ctx, "slowlog", "len").Result()

	// get slow log info
	ret, err := rc.SlowLogGet(ctx, nums.(int64)).Result()

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

// show by table
func ShowTable(data []SlowLog, sortColumn string) {
	if len(data) == 0 {
		fmt.Println("this redis has no slow log!")
		os.Exit(0)
	}

	//create new table
	table := simpletable.New()
	//set table header
	table.Header = &simpletable.Header{
		Cells: []*simpletable.Cell{
			{Align: simpletable.AlignCenter, Text: "ID"},
			{Align: simpletable.AlignCenter, Text: "Instance"},
			{Align: simpletable.AlignCenter, Text: "Command"},
			{Align: simpletable.AlignCenter, Text: "Duration"},
			{Align: simpletable.AlignCenter, Text: "Time"},
		},
	}

	dataSort(sortColumn, data)
	var cnt = 1
	// 遍历 data,把数据放入 table
	for _, rowMap := range data {
		row := []interface{}{
			strconv.Itoa(cnt),
			rowMap.Instance,
			rowMap.Command,
			rowMap.Duration.String(),
			rowMap.Time,
		}
		r := []*simpletable.Cell{
			{Text: row[0].(string)},
			{Text: row[1].(string)},
			{Text: row[2].(string)},
			{Text: row[3].(string)},
			{Text: row[4].(string)},
		}

		table.Body.Cells = append(table.Body.Cells, r)
		cnt += 1
	}

	table.SetStyle(simpletable.StyleUnicode)
	fmt.Println(table.String())
}

func Run(instance, redisType, sortBy string) {
	// get all slow log info
	var slowLogs []SlowLog
	switch {
	case redisType == "standalone": //standalone type
		ret, err := getSlowLog(instance)
		if err != nil {
			fmt.Printf("get slowlog failed, err:%v\n", err)
			return
		}
		slowLogs = append(slowLogs, ret...)
	case redisType == "cluster":
		//get all instance list from cluster
		instances, err := getAllInstance(instance)
		if err != nil {
			fmt.Printf("get all instances failed, err:%v\n", err)
			return
		}

		for _, instance := range instances { //cluster type
			ret, err := getSlowLog(instance)
			if err != nil {
				fmt.Printf("get slowlog failed, err:%v\n", err)
				return
			}
			slowLogs = append(slowLogs, ret...)
		}
	}

	// show the slow log info by table
	ShowTable(slowLogs, sortBy)
}
