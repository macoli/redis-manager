package main

import (
	"fmt"
	"os"

	"github.com/macoli/redis-manager/cmd/check"

	"github.com/macoli/redis-manager/cmd/clusterConfig"
	"github.com/macoli/redis-manager/cmd/clusterDataClear"
	"github.com/macoli/redis-manager/cmd/moveSlot"
	"github.com/macoli/redis-manager/cmd/showClusterMap"
	"github.com/macoli/redis-manager/cmd/showSlowLog"
)

func Usage() {
	fmt.Fprintf(os.Stdout, `Usage of redis-manager:
Options:
	slowlog         慢查询信息展示
	moveslot        集群迁移指定的slot到指定节点
	clustermap      集群映射关系展示
	clusterclear    集群数据清空
	clusterconfig   集群配置相关:获取配置项,批量修改配置项
	check           redis 状态检查
`)
}
func main() {
	if len(os.Args) < 2 {
		Usage()
		os.Exit(0)
	}
	switch os.Args[1] {
	case "slowlog":
		showSlowLog.Run()
	case "clustermap":
		showClusterMap.Run()
	case "moveslot":
		moveSlot.Run()
	case "clusterclear":
		clusterDataClear.Run()
	case "clusterconfig":
		clusterConfig.Run()
	case "check":
		check.Run()
	default:
		Usage()
		os.Exit(0)
	}
}
