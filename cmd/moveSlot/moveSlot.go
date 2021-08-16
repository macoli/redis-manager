package moveSlot

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/macoli/redis-manager/cmd/paramDeal"
	"github.com/macoli/redis-manager/pkg/redis"
)

//迁移指定 slot 到指定节点
//1.指定 slot,slot所在源节点,slot 要迁移的目的节点
//2.对目标节点发送 cluster setslot [slot] importing [source nodeID]
//3.对源节点发送 cluster setslot [slot] migrating [target nodeID]
//4.源节点循环执行 cluster getkeysinslot [slot] [count]  --获取 count 个属于 slot 的键
//5.源节点执行批量迁移 key 的命令 migrate [target ip] [target port] "" 0 [timeout] keys [keys...]
//6.重复执行步骤 4 和 5,直到 slot 的所有数据都迁移到目标节点
//7.向集群内所有主节点发送 cluster setslot [slot] node [target nodeID],以通知 slot 已经分配给了目标节点

func Param() (string, string, string, string, int) {
	moveSlot := flag.NewFlagSet("clustermap", flag.ExitOnError)
	sourceAddr := moveSlot.String("saddr", "127.0.0.1:6379", "要迁移slot的源地址")
	targetAddr := moveSlot.String("taddr", "127.0.0.1:6379", "要迁移slot的目的地址")
	password := moveSlot.String("pass", "", "redis集群密码,默认为空")
	slot := moveSlot.String("slot", "", "需要迁移的slot,范围:0-16384,"+
		"格式:1,100-100,355,2000-2002,默认迁移源 redis 的所有 slot")
	count := moveSlot.Int("count", 1000, "每次迁移key的数量")
	paramDeal.ParamsCheck(moveSlot)

	return *sourceAddr, *targetAddr, *password, *slot, *count
}

// 校验获取到的 slotStr,并格式化
func formatSlotStr(slotStr string) (slots []int64) {
	// 如果 slotStr 是数字,说明此次只迁移一个 slot
	slot, er := strconv.ParseInt(slotStr, 10, 64)
	if er == nil {
		redis.CheckSlot(slot) // 校验 slot 是否在 0-16384
		slots = append(slots, slot)
		return
	}
	// 如果 slotStr 是非数字,校验格式是否正确(1,100-100,355,2000-2002),并格式化
	for _, item := range strings.Split(slotStr, ",") {
		if strings.Contains(item, "-") { // 格式化类型: "1-100"
			start, sErr := strconv.ParseInt(strings.Split(item, "-")[0], 10, 64)
			end, eErr := strconv.ParseInt(strings.Split(item, "-")[1], 10, 64)
			if sErr != nil || eErr != nil {
				fmt.Printf("格式化slotStr: %s 失败\n", item)
				return
			}
			for i := start; i <= end; i++ {
				redis.CheckSlot(i)
				slots = append(slots, i)
			}

		} else {
			slot, err := strconv.ParseInt(item, 10, 64)
			if err == nil { // 格式化类型: "1111"
				redis.CheckSlot(slot) // 校验 slot 是否在 0-16384
				slots = append(slots, slot)
				return
			} else { // 非法字符
				fmt.Printf("格式化slotStr: %s 失败\n", item)
				return
			}

		}
	}

	return
}

// Run 迁移指定 slot 到指定节点的执行函数
func Run() {
	sourceAddr, targetAddr, password, slotStr, count := Param()

	//获取集群节点信息:addr nodeID,并判断sourceAddr 和 targetAddr 在同一个集群
	data, err := redis.FormatClusterNodes(sourceAddr, password)
	if err != nil {
		fmt.Printf("获取集群所有master节点信息失败, err:%v\n", err)
		return
	}
	if _, ok := data.AddrToID[targetAddr]; !ok {
		fmt.Printf("源节点和目标节点不在一个集群")
		return
	}

	var slots []int64
	if slotStr == "" { // 如果 slotStr 为空,获取 sourceAddr 上所有的 slot
		slots = redis.GetAddrAllSlots(data, sourceAddr)
	} else { // 校验并格式化slotStr
		slots = formatSlotStr(slotStr)
	}

	redis.MoveSlot(sourceAddr, targetAddr, password, slots, count, data)
}
