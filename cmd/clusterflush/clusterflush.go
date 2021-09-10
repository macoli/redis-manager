package clusterflush

import (
	"fmt"
	"time"

	"github.com/macoli/redis-manager/pkg/param"

	"github.com/macoli/redis-manager/pkg/iredis"
)

func Run() {
	addr, password, flushCMD, flushWorker := param.ClusterFlush()

	// 获取集群信息
	data, err := iredis.ClusterInfoFormat(addr, password)
	if err != nil {
		fmt.Printf("获取集群节点信息失败: %v\n", err)
		return
	}

	fmt.Printf("========== Cluster Data FLUSH ==========\n")
	startTime := time.Now()
	fmt.Printf("Start Time: %v\n", startTime.Format("2006-01-02 15:04:05"))

	// 清空集群所有节点
	iredis.ClusterFlush(data, password, flushCMD, flushWorker)

	endTime := time.Now()
	fmt.Printf("End Time: %v\n", endTime.Format("2006-01-02 15:04:05"))
	cost := endTime.Sub(startTime)
	fmt.Printf("Cost Time: %v\n", cost)
}
