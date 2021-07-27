package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/macoli/redis-manager/cmd/clusterConfig"

	"github.com/macoli/redis-manager/cmd/clusterDataClear"

	"github.com/macoli/redis-manager/cmd/moveSlot"

	"github.com/macoli/redis-manager/cmd/showClusterMap"

	"github.com/macoli/redis-manager/cmd/showSlowLog"
)

// redis manager project

func usage() {
	fmt.Fprintf(os.Stdout, `Usage of redis-manager:
Options:
	slowlog         慢查询信息展示
	moveslot        集群迁移指定的slot到指定节点(仅支持迁移单个slot)
	clustermap      集群映射关系展示
	clusterclear    集群数据清空
	clusterconfig   集群配置相关:获取配置项,批量修改配置项
`)
}

func paramsCheck(p *flag.FlagSet) {
	params := os.Args[2:]
	if len(params) == 0 {
		p.Usage()
		os.Exit(0)
	}
	if err := p.Parse(os.Args[2:]); err != nil {
		fmt.Printf("参数解析失败, err:%v\n", err)
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
	sAddr := ss.String("addr", "127.0.0.1:6379", "redis地址")
	sPass := ss.String("pass", "", "redis密码")
	sRedisType := ss.String("type", "standalone", "redis类型选择:standalone/cluster")
	sSortBy := ss.String("sortby", "Time", "按不通列排序:Instance/Command/Duration/Time")
	//cluster map args
	cm := flag.NewFlagSet("clustermap", flag.ExitOnError)
	cAddr := cm.String("addr", "127.0.0.1:6379", "redis地址")
	cPass := cm.String("pass", "", "redis密码")
	cSortBy := cm.String("sortby", "MasterAddr", "按不通列排序:MasterID/MasterAddr/slaveAddr/slaveID")
	//move slot args
	ms := flag.NewFlagSet("clustermap", flag.ExitOnError)
	mSourceAddr := ms.String("saddr", "127.0.0.1:6379", "要迁移slot的源地址")
	mTargetAddr := ms.String("taddr", "127.0.0.1:6379", "要迁移slot的目的地址")
	mPass := ms.String("pass", "", "redis集群密码,默认为空")
	mSlot := ms.Int("slot", -1, "需要迁移的slot,范围:0-16384")
	mCount := ms.Int("count", 100, "每次迁移key的数量")
	//cluster data clear args
	cdc := flag.NewFlagSet("clusterclear", flag.ExitOnError)
	cdcAddr := cdc.String("addr", "127.0.0.1:6379", "redis地址")
	cdcPass := cdc.String("password", "", "redis密码")
	cdcFlushCMD := cdc.String("flushcmd", "FLUSHALL", "清空命令,当清空命令被重命名后使用")
	//cluster config args
	cc := flag.NewFlagSet("clusterconfig", flag.ExitOnError)
	ccAddr := cc.String("addr", "127.0.0.1:6379", "redis地址")
	ccPass := cc.String("password", "", "redis集群密码")
	ccType := cc.String("type", "get", "操作的类型,可选项:set/get")
	ccArg := cc.String("config", "", "操作的配置项")
	ccValue := cc.String("value", "", "设置集群配置项时,配置项的值.仅当吵着类型是set时生效")

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
			fmt.Printf("slot 值必须在0-16384")
			os.Exit(1)
		}
		moveSlot.Run(*mSourceAddr, *mTargetAddr, *mPass, *mSlot, *mCount)
	case "clusterclear":
		paramsCheck(cdc)
		clusterDataClear.Run(*cdcAddr, *cdcPass, *cdcFlushCMD)
	case "clusterconfig":
		paramsCheck(cc)
		clusterConfig.Run(*ccAddr, *ccPass, *ccType, *ccArg, *ccValue)
	default:
		usage()
		os.Exit(0)
	}

}
