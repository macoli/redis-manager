package moveslot

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/macoli/redis-manager/pkg/param"

	"github.com/macoli/redis-manager/pkg/redis"
)

// formatSlotStr 校验获取到的 slotStr,并格式化
func formatSlotStr(slotStr string) (slots []int64, err error) {
	// 如果 slotStr 是数字,说明此次只迁移一个 slot
	slot, er := strconv.ParseInt(slotStr, 10, 64)
	if er == nil {
		redis.SlotCheck(slot) // 校验 slot 是否在 0-16384
		slots = append(slots, slot)
		return
	}
	// 如果 slotStr 是非数字,校验格式是否正确(1,100-100,355,2000-2002),并格式化
	for _, item := range strings.Split(slotStr, ",") {
		if strings.Contains(item, "-") { // 格式化类型: "1-100"
			start, sErr := strconv.ParseInt(strings.Split(item, "-")[0], 10, 64)
			end, eErr := strconv.ParseInt(strings.Split(item, "-")[1], 10, 64)
			if sErr != nil || eErr != nil {
				errMsg := fmt.Sprintf("格式化参数 slotStr: %s 失败\n", item)
				return nil, errors.New(errMsg)
			}
			for i := start; i <= end; i++ {
				redis.SlotCheck(i)
				slots = append(slots, i)
			}

		} else {
			slot, err := strconv.ParseInt(item, 10, 64)
			if err == nil { // 格式化类型: "1111"
				redis.SlotCheck(slot) // 校验 slot 是否在 0-16384
				slots = append(slots, slot)
				return slots, nil
			} else { // 非法字符
				fmt.Printf("格式化slotStr: %s 失败\n", item)
				return nil, err
			}

		}
	}

	return
}

// Run 迁移指定 slot 到指定节点的执行函数
func Run() {
	sourceAddr, targetAddr, password, slotStr, count := param.MoveSlot()

	//获取集群节点信息:addr nodeID,并判断sourceAddr 和 targetAddr 在同一个集群
	data, err := redis.ClusterInfoFormat(sourceAddr, password)
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
		slots, err = redis.SlotsGetByInstance(data, sourceAddr)
		if err != nil {
			fmt.Printf("获取源节点 %s 的 slots 信息失败, err:%v\n", sourceAddr, err)
		}
	} else { // 校验并格式化slotStr
		slots, err = formatSlotStr(slotStr)
		if err != nil {
			fmt.Printf("格式化参数 slotStr: %s 失败, err:%v\n", slotStr, err)
		}
	}

	// 建立到 sourceAddr 的连接
	sourceClient, err := redis.InitStandConn(sourceAddr, password)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 建立到 targetAddr 的连接
	targetClient, err := redis.InitStandConn(targetAddr, password)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 函数结束后关闭 redis 连接
	defer sourceClient.Close()
	defer targetClient.Close()

	redis.SlotMove(sourceAddr, targetAddr, password, slots, count, data)

}