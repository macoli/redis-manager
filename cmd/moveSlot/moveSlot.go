package moveSlot

import (
	"flag"
	"fmt"
	"os"

	"github.com/macoli/redis-manager/cmd/paramDeal"

	r "github.com/macoli/redis-manager/pkg/redis"
)

//迁移指定 slot 到指定节点
//1.指定 slot,slot所在源节点,slot 要迁移的目的节点
//2.对目标节点发送 cluster setslot [slot] importing [source nodeID]
//3.对源节点发送 cluster setslot [slot] migrating [target nodeID]
//4.源节点循环执行 cluster getkeysinslot [slot] [count]  --获取 count 个属于 slot 的键
//5.源节点执行批量迁移 key 的命令 migrate [target ip] [target port] "" 0 [timeout] keys [keys...]
//6.重复执行步骤 4 和 5,直到 slot 的所有数据都迁移到目标节点
//7.向集群内所有主节点发送 cluster setslot [slot] node [target nodeID],以通知 slot 已经分配给了目标节点

func Param() (string, string, string, int, int) {
	moveSlot := flag.NewFlagSet("clustermap", flag.ExitOnError)
	sourceAddr := moveSlot.String("saddr", "127.0.0.1:6379", "要迁移slot的源地址")
	targetAddr := moveSlot.String("taddr", "127.0.0.1:6379", "要迁移slot的目的地址")
	password := moveSlot.String("pass", "", "redis集群密码,默认为空")
	slot := moveSlot.Int("slot", -1, "需要迁移的slot,范围:0-16384")
	count := moveSlot.Int("count", 100, "每次迁移key的数量")
	paramDeal.ParamsCheck(moveSlot)

	if *slot < 0 || *slot > 16384 {
		fmt.Printf("slot 值必须在0-16384")
		os.Exit(1)
	}

	return *sourceAddr, *targetAddr, *password, *slot, *count
}

// Run 迁移指定 slot 到指定节点的执行函数
func Run() {
	sourceAddr, targetAddr, password, slot, count := Param()

	//获取集群节点信息:addr nodeID,并判断sourceAddr 和 targetAddr 在同一个集群
	data, err := r.FormatClusterNodes(sourceAddr, password)
	if err != nil {
		fmt.Printf("获取集群所有master节点信息失败, err:%v\n", err)
		return
	}
	if _, ok := data.AddrToID[targetAddr]; !ok {
		fmt.Printf("源节点和目标节点不在一个集群")
		return
	}

	fmt.Printf("Slot %d 开始迁移\n", slot)
	fmt.Printf("SLOT %d\n", slot)
	fmt.Printf("FROM sourceAddr: %s sourceNodeID: %s\n", sourceAddr, data.AddrToID[sourceAddr])
	fmt.Printf("TO targetAddr: %s targetNodeID: %s\n", targetAddr, data.AddrToID[targetAddr])

	//对目标节点importing
	err = r.SetSlotImporting(targetAddr, password, slot, data.AddrToID[sourceAddr])
	if err != nil {
		fmt.Printf("在目标节点执行命令:set slot importing 失败, err:%v\n", err)
		return
	}

	//对源节点 migration
	err = r.SetSlotMigrating(sourceAddr, password, slot, data.AddrToID[targetAddr])
	if err != nil {
		fmt.Printf("在源节点执行命令:set slot migration 失败, err:%v\n", err)
		return
	}

	//对源节点migrate 迁移 slot 数据
	err = r.MoveData(sourceAddr, targetAddr, password, slot, count)
	if err != nil {
		fmt.Printf("move the slot data from source addr to the target addr failed, err:%v\n", err)
		fmt.Printf("迁移 slot %d 的数据失败 err:%v\n", slot, err)
		return
	}

	//通告整个集群 slot 已迁移到目标节点
	err = r.SetSlotNode(data.Masters, password, slot, data.AddrToID[targetAddr])
	if err != nil {
		fmt.Printf("为集群所有节点执行命令:set slot 失败, err:%v\n", err)
		return
	}
	fmt.Printf("Slot %d 完成迁移\n", slot)
}
