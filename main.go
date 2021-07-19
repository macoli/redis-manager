package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/macoli/redis-manager/cmd/showClusterMap"

	"github.com/macoli/redis-manager/cmd/showSlowLog"
)

// redis manager project

func usage() {
	fmt.Fprintf(os.Stderr, `Usage of redis-manager:
Options:
  slowlog redis慢查询信息展示
  clustermap redis cluster集群展示
`)
}

func paramsCheck(p *flag.FlagSet) {
	params := os.Args[2:]
	if len(params) == 0 {
		p.Usage()
		os.Exit(1)
	}
	if err := p.Parse(os.Args[2:]); err != nil {
		fmt.Printf("get params failed, err:%v\n", err)
		p.Usage()
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	// get args from cmd
	// slow log args
	ss := flag.NewFlagSet("slowlog", flag.ExitOnError)
	sAddr := ss.String("addr", "127.0.0.1:6379", "redis addr")
	sRedisType := ss.String("type", "standalone", "redis type:standalone/cluster")
	sSortBy := ss.String("sortby", "Time", "sortBy:Instance/Command/Duration/Time")

	cm := flag.NewFlagSet("clustermap", flag.ExitOnError)
	cAddr := cm.String("addr", "127.0.0.1:6379", "redis addr")
	cSortBy := cm.String("sortby", "MasterIP", "sortBy:MasterID/MasterIP/slaveIP/slaveID")

	switch os.Args[1] {
	case "slowlog":
		paramsCheck(ss)
		showSlowLog.Run(*sAddr, *sRedisType, *sSortBy)
	case "clustermap":
		paramsCheck(cm)
		showClusterMap.Run(*cAddr, *cSortBy)
	default:
		usage()
		os.Exit(1)
	}

}
