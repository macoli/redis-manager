package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/macoli/redis-manager/cmd/moveSlot"

	"github.com/macoli/redis-manager/cmd/showClusterMap"

	"github.com/macoli/redis-manager/cmd/showSlowLog"
)

// redis manager project

func usage() {
	fmt.Fprintf(os.Stdout, `Usage of redis-manager:
Options:
  slowlog    慢查询信息展示
  clustermap 集群映射关系展示
  moveslot   集群迁移指定的slot到指定节点(仅支持迁移单个slot)
`)
}

func paramsCheck(p *flag.FlagSet) {
	params := os.Args[2:]
	if len(params) == 0 {
		p.Usage()
		os.Exit(0)
	}
	if err := p.Parse(os.Args[2:]); err != nil {
		fmt.Printf("parse params failed, err:%v\n", err)
		p.Usage()
		os.Exit(0)
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(0)
	}

	// get args from cmd
	// slow log args
	ss := flag.NewFlagSet("slowlog", flag.ExitOnError)
	sAddr := ss.String("addr", "127.0.0.1:6379", "redis addr")
	sPass := ss.String("pass", "", "redis password")
	sRedisType := ss.String("type", "standalone", "redis type:standalone/cluster")
	sSortBy := ss.String("sortby", "Time", "sortBy:Instance/Command/Duration/Time")
	//cluster map args
	cm := flag.NewFlagSet("clustermap", flag.ExitOnError)
	cAddr := cm.String("addr", "127.0.0.1:6379", "redis addr")
	cPass := cm.String("pass", "", "redis password")
	cSortBy := cm.String("sortby", "MasterAddr", "sortBy:MasterID/MasterAddr/slaveAddr/slaveID")
	//move slot args
	ms := flag.NewFlagSet("clustermap", flag.ExitOnError)
	mSourceAddr := ms.String("saddr", "127.0.0.1:6379", "redis cluster source addr")
	mTargetAddr := ms.String("taddr", "127.0.0.1:6379", "redis cluster target addr")
	mPass := ms.String("pass", "", "redis cluster password")
	mSlot := ms.Int("slot", -1, "the slot to be migrated")
	mCount := ms.Int("count", 100, "number of keys to be migrated each time")

	switch os.Args[1] {
	case "slowlog":
		paramsCheck(ss)
		showSlowLog.Run(*sAddr, *sPass, *sRedisType, *sSortBy)
	case "clustermap":
		paramsCheck(cm)
		showClusterMap.Run(*cAddr, *cPass, *cSortBy)
	case "moveslot":
		paramsCheck(ms)
		if *mSlot < 0 || *mSlot > 16384 {
			fmt.Printf("the slot parameter must be between 0-16384")
			os.Exit(1)
		}
		moveSlot.Run(*mSourceAddr, *mTargetAddr, *mPass, *mSlot, *mCount)
	default:
		usage()
		os.Exit(0)
	}

}
