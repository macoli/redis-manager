package redis

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

////SetSlotImporting 对目标节点执行 cluster setslot [slot] importing [source nodeID]
//func SetSlotImporting(targetAddr string, password string, slot int, sourceNodeID string) (err error) {
//	rc, err := InitStandRedis(targetAddr, password)
//	if err != nil {
//		return
//	}
//	defer rc.Close()
//	_, err = rc.Do(ctx, "cluster", "setslot", slot, "importing", sourceNodeID).Result()
//	if err != nil {
//		return
//	}
//	return nil
//}
//
////SetSlotMigrating 对源节点发送 cluster setslot [slot] migrating [target nodeID]
//func SetSlotMigrating(sourceAddr string, password string, slot int, targetNodeID string) (err error) {
//	rc, err := InitStandRedis(sourceAddr, password)
//	if err != nil {
//		return err
//	}
//	defer rc.Close()
//	_, err = rc.Do(ctx, "cluster", "setslot", slot, "migrating", targetNodeID).Result()
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
////MoveData 迁移 slot 数据
//func MoveData(sourceAddr string, targetAddr string, password string, slot int, count int) (err error) {
//	//连接源节点
//	rc, err := InitStandRedis(sourceAddr, password)
//	if err != nil {
//		return err
//	}
//	defer rc.Close()
//
//	//获取目标节点的 ip 和 port
//	targetIP := strings.Split(targetAddr, ":")[0]
//	targetPort := strings.Split(targetAddr, ":")[1]
//
//	//循环迁移 slot 的数据到目标节点
//	for {
//		ret := rc.ClusterGetKeysInSlot(ctx, slot, count)
//		//fmt.Printf("%#v\n", ret)
//		for _, key := range ret.Val() {
//			_, err := rc.Migrate(ctx, targetIP, targetPort, key, 0, time.Second*10).Result()
//			if err != nil {
//				return err
//			}
//			//fmt.Printf("migrate result:%v\n", ret)
//			fmt.Printf("#")
//		}
//		if len(ret.Val()) < count {
//			fmt.Printf("\n")
//			break
//		}
//	}
//	return nil
//}
//
////SetSlotNode 向集群内所有主节点发送 cluster setslot [slot] node [target nodeID],以通知 slot 已经分配给了目标节点
//func SetSlotNode(clusterNodes []string, password string, slot int, targetNodeID string) (err error) {
//	for _, addr := range clusterNodes {
//		rc, err := InitStandRedis(addr, password)
//		if err != nil {
//			return err
//		}
//
//		_, err = rc.Do(ctx, "cluster", "setslot", slot, "node", targetNodeID).Result()
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}

// CheckSlot 校验 slot 是否合法范围中: 0-16384
func CheckSlot(slot int64) {
	if slot < 0 || slot > 16384 {
		fmt.Printf("slot 值必须在: 0-16384")
		os.Exit(1)
	}
}

// GetAddrAllSlots 从 ClusterNodesMap 中获取对应 redis 实例的所有 slot
func GetAddrAllSlots(data *ClusterNodesMap, addr string) (slots []int64) {
	// 获取对应 addr 的 slotStr 信息
	var slotStr string
	for _, Node := range data.ClusterNodesDetail {
		if Node.MasterAddr == addr {
			slotStr = Node.SlotStr
		}
	}

	// 格式化获取到的 slotStr 信息
	for _, item := range strings.Split(slotStr, " ") {
		if len(item) == 0 {
			continue
		}

		if strings.Contains(item, "-") { // 格式化类型: "1-100"
			start, sErr := strconv.ParseInt(strings.Split(item, "-")[0], 10, 64)
			end, eErr := strconv.ParseInt(strings.Split(item, "-")[1], 10, 64)
			if sErr != nil || eErr != nil {
				fmt.Printf("格式化slotStr: %s 失败\n", item)
				return
			}
			for i := start; i <= end; i++ {
				CheckSlot(i)
				slots = append(slots, i)
			}

		} else { // 格式化类型: "1111"
			slot, err := strconv.ParseInt(item, 10, 64)
			if err != nil {
				fmt.Printf("格式化slotStr: %s 失败\n", item)
				return
			}
			CheckSlot(slot)
			slots = append(slots, slot)
		}
	}
	return
}

// MoveSlot 迁移 slot
func MoveSlot(sourceAddr, targetAddr, password string, slots []int64, count int, data *ClusterNodesMap) {
	// 建立到 sourceAddr 的连接
	sourceClient, err := InitStandRedis(sourceAddr, password)
	if err != nil {
		fmt.Printf("连接源地址: %s 失败, err:%v\n", sourceAddr, err)
		return
	}

	// 建立到 targetAddr 的连接
	targetClient, err := InitStandRedis(targetAddr, password)
	if err != nil {
		fmt.Printf("连接目标地址地址: %s 失败, err:%v\n", targetAddr, err)
		return
	}

	// 函数结束后关闭 redis 连接
	defer sourceClient.Close()
	defer targetClient.Close()

	// 迁移 slot
	for _, slot := range slots {
		// 打印帮助信息
		fmt.Printf("Slot %d 开始迁移\n", slot)
		fmt.Printf("FROM sourceAddr: %s sourceNodeID: %s\n", sourceAddr, data.AddrToID[sourceAddr])
		fmt.Printf("TO targetAddr: %s targetNodeID: %s\n", targetAddr, data.AddrToID[targetAddr])

		// 对目标节点importing 命令: cluster setslot [slot] importing [source nodeID]
		_, err = targetClient.Do(ctx, "cluster", "setslot", slot, "importing", data.AddrToID[sourceAddr]).Result()
		if err != nil {
			fmt.Printf("在目标节点执行命令:set slot importing 失败, err:%v\n", err)
			return
		}

		// 对源节点 migration 命令: cluster setslot [slot] migrating [target nodeID]
		_, err = sourceClient.Do(ctx, "cluster", "setslot", slot, "migrating", data.AddrToID[targetAddr]).Result()
		if err != nil {
			fmt.Printf("在源节点执行命令:set slot migration 失败, err:%v\n", err)
			return
		}

		// 迁移 slot 中的数据
		//获取目标节点的 ip 和 port
		targetIP := strings.Split(targetAddr, ":")[0]
		targetPort := strings.Split(targetAddr, ":")[1]
		//循环迁移 slot 的数据到目标节点
		for {
			ret := sourceClient.ClusterGetKeysInSlot(ctx, int(slot), count) // 从源节点获取 slot 的 key(批量)
			// 循环将获取的 key 发往目标 redis 实例
			for _, key := range ret.Val() {
				_, err := sourceClient.Migrate(ctx, targetIP, targetPort, key, 0, time.Second*10).Result()
				if err != nil {
					fmt.Printf("迁移slot: %d 中的数据失败, err:%v\n", slot, err)
					return
				}
				//fmt.Printf("#")  // 打印迁移 key 进度
			}
			if len(ret.Val()) < count {
				//fmt.Printf("\n")
				break
			}
		}

		// 通告集群slot 已经分配给了目标节点,向集群内所有主节点发送命令: cluster setslot [slot] node [target nodeID]
		for _, addr := range data.Masters {
			rc, err := InitStandRedis(addr, password)
			if err != nil {
				fmt.Printf("连接 redis: %s 失败, err:%v\n", addr, err)
				return
			}

			_, err = rc.Do(ctx, "cluster", "setslot", slot, "node", data.AddrToID[targetAddr]).Result()
			if err != nil {
				fmt.Printf("在 redis: %s 上执行命令: cluster setslot 失败, err:%v\n", addr, err)
				return
			}
			rc.Close()
		}

		fmt.Printf("Slot %d 完成迁移\n", slot)
	}

}
