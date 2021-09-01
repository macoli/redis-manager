package moveSlot

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/macoli/redis-manager/cmd/paramDeal"
	"github.com/macoli/redis-manager/pkg/redis"
)

func Param() (string, string, string, string, int) {
	moveSlot := flag.NewFlagSet("moveslot", flag.ExitOnError)
	sourceAddr := moveSlot.String("saddr", "127.0.0.1:6379", "要迁移slot的源地址")
	targetAddr := moveSlot.String("taddr", "127.0.0.1:6379", "要迁移slot的目的地址")
	password := moveSlot.String("pass", "", "redis集群密码,默认为空")
	slot := moveSlot.String("slot", "", "需要迁移的slot,范围:0-16384,"+
		"格式:1,100-100,355,2000-2002,默认迁移源 redis 的所有 slot")
	count := moveSlot.Int("count", 1000, "每次迁移key的数量")
	paramDeal.ParamsCheck(moveSlot)

	return *sourceAddr, *targetAddr, *password, *slot, *count
}

// formatSlotStr 校验获取到的 slotStr,并格式化
func formatSlotStr(slotStr string) (slots []int64, err error) {
	// 如果 slotStr 是数字,说明此次只迁移一个 slot
	slot, er := strconv.ParseInt(slotStr, 10, 64)
	if er == nil {
		redis.CheckSlotParam(slot) // 校验 slot 是否在 0-16384
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
				redis.CheckSlotParam(i)
				slots = append(slots, i)
			}

		} else {
			slot, err := strconv.ParseInt(item, 10, 64)
			if err == nil { // 格式化类型: "1111"
				redis.CheckSlotParam(slot) // 校验 slot 是否在 0-16384
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
		slots, err = redis.GetClusterAddrSlots(data, sourceAddr)
		if err != nil {
			fmt.Printf("获取源节点 %s 的 slots 信息失败, err:%v\n", sourceAddr, err)
		}
	} else { // 校验并格式化slotStr
		slots, err = formatSlotStr(slotStr)
		if err != nil {
			fmt.Printf("格式化参数 slotStr: %s 失败, err:%v\n", slotStr, err)
		}
	}

	err = redis.MoveSlot(sourceAddr, targetAddr, password, slots, count, data)
	if err != nil {
		fmt.Printf("迁移 slot 失败, err:%v\n", err)
	}
}
