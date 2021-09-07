package param

import (
	"flag"
	"fmt"
	"os"
)

// paramsCheck 校验接收到的参数
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

// SlowLog 参数获取
func SlowLog() (string, string, string, string) {
	slowLog := flag.NewFlagSet("slowlog", flag.ExitOnError)
	addr := slowLog.String("a", "127.0.0.1:6379", "address, redis地址")
	password := slowLog.String("p", "", "password, redis密码")
	redisType := slowLog.String("t", "standalone", "type, redis类型选择:standalone/cluster")
	sortBy := slowLog.String("s", "Time", "sort, 按列排序:Instance/Command/Duration/Time")
	paramsCheck(slowLog)
	return *addr, *password, *redisType, *sortBy
}

// ClusterMap 参数获取
func ClusterMap() (string, string, string) {
	clusterMap := flag.NewFlagSet("clustermap", flag.ExitOnError)
	addr := clusterMap.String("a", "127.0.0.1:6379", "address, redis地址")
	password := clusterMap.String("p", "", "password, redis密码")
	sortBy := clusterMap.String("s", "MasterAddr",
		"sort, 按列排序:MasterID/MasterAddr/slaveAddr/slaveID")
	paramsCheck(clusterMap)
	return *addr, *password, *sortBy
}

// MoveSlot 参数获取
func MoveSlot() (string, string, string, string, int) {
	moveSlot := flag.NewFlagSet("moveslot", flag.ExitOnError)
	sourceAddr := moveSlot.String("from", "127.0.0.1:6379", "要迁移slot的源地址")
	targetAddr := moveSlot.String("to", "127.0.0.1:6379", "要迁移slot的目的地址")
	password := moveSlot.String("p", "", "password, redis集群密码,默认为空")
	slot := moveSlot.String("slots", "", "需要迁移的slot,范围:0-16384,"+
		"格式:1,100-100,355,2000-2002,默认迁移源 redis 的所有 slot")
	count := moveSlot.Int("count", 1000, "每次迁移key的数量")
	paramsCheck(moveSlot)

	return *sourceAddr, *targetAddr, *password, *slot, *count
}

// ClusterFlush 获取参数
func ClusterFlush() (string, string, string) {
	clusterDataClear := flag.NewFlagSet("clusterflush", flag.ExitOnError)
	addr := clusterDataClear.String("a", "127.0.0.1:6379", "address, redis地址")
	password := clusterDataClear.String("p", "", "password, redis密码")
	flushCMD := clusterDataClear.String("c", "FLUSHALL",
		"flush command, 清空redis命令,当清空命令被重命名后使用")
	paramsCheck(clusterDataClear)

	return *addr, *password, *flushCMD
}

// ClusterConfig 获取参数
func ClusterConfig() (string, string, string, string, string) {
	clusterConfig := flag.NewFlagSet("clusterconfig", flag.ExitOnError)
	addr := clusterConfig.String("a", "127.0.0.1:6379", "address, redis地址")
	password := clusterConfig.String("p", "", "password, redis集群密码")
	opType := clusterConfig.String("t", "get", "type, 操作的类型,可选项:set/get")
	config := clusterConfig.String("ck", "", "config key, 配置项的 key")
	setValue := clusterConfig.String("cv", "", "config value, 设置集群配置项时,配置项的值.仅当吵着类型是set时生效")
	paramsCheck(clusterConfig)

	return *addr, *password, *opType, *config, *setValue
}

// param 获取参数
func Check() (string, string, string) {
	checkConfig := flag.NewFlagSet("check", flag.ExitOnError)
	addr := checkConfig.String("a", "127.0.0.1:6379", "address, redis地址")
	password := checkConfig.String("p", "", "password, redis密码")
	redisType := checkConfig.String("t", "standalone", "type, redis 类型,可选项:standalone/sentinel/cluster")
	paramsCheck(checkConfig)

	return *addr, *password, *redisType
}
