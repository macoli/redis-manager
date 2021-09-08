package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// SlotCheck 校验 slot 是否合法范围中: 0-16384
func SlotCheck(slot int64) {
	if slot < 0 || slot > 16384 {
		fmt.Printf("slot 值必须在: 0-16384")
		os.Exit(1)
	}
}

// SlotsGetByInstance 从 FormatClusterInfo 中获取对应 redis 实例的所有 slot
func SlotsGetByInstance(data *ClusterInfo, addr string) (slots []int64, err error) {
	// 获取对应 addr 的 slotStr 信息
	var slotStr string
	for _, Node := range data.MasterSlaveMaps {
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
				errMsg := fmt.Sprintf("格式化 %s 的 slot 信息 %s 失败\n", addr, item)
				return nil, errors.New(errMsg)
			}
			for i := start; i <= end; i++ {
				SlotCheck(i)
				slots = append(slots, i)
			}

		} else { // 格式化类型: "1111"
			slot, err := strconv.ParseInt(item, 10, 64)
			if err != nil {
				errMsg := fmt.Sprintf("格式化 %s 的 slot 信息 %s 失败\n", addr, item)
				return nil, errors.New(errMsg)
			}
			SlotCheck(slot)
			slots = append(slots, slot)
		}
	}
	return
}

// move 迁移单个 slot
func move(sourceClient, targetClient *redis.Client, sourceAddr, targetAddr, password string,
	slot int64, count int, data *ClusterInfo) {

	defer redisWG.Done()
	// 打印帮助信息
	fmt.Printf("Slot %d 开始迁移\n", slot)

	// 对目标节点importing 命令: cluster setslot [slot] importing [source nodeID]
	_, err := targetClient.Do(context.Background(), "cluster", "setslot", slot, "importing", data.AddrToID[sourceAddr]).Result()
	if err != nil {
		fmt.Printf("在目标节点执行命令:set slot importing 失败: %v\n", err)
		return
	}

	// 对源节点 migration 命令: cluster setslot [slot] migrating [target nodeID]
	_, err = sourceClient.Do(context.Background(), "cluster", "setslot", slot, "migrating", data.AddrToID[targetAddr]).Result()
	if err != nil {
		fmt.Printf("在源节点执行命令:set slot migration 失败: %v\n", err)
		return
	}

	// 迁移 slot 中的数据
	//获取目标节点的 ip 和 port
	targetIP := strings.Split(targetAddr, ":")[0]
	targetPort := strings.Split(targetAddr, ":")[1]
	//循环迁移 slot 的数据到目标节点
	for {
		ret := sourceClient.ClusterGetKeysInSlot(context.Background(), int(slot), count) // 从源节点获取 slot 的 key(批量)
		// 循环将获取的 key 发往目标 redis 实例
		for _, key := range ret.Val() {
			_, err := sourceClient.Migrate(context.Background(), targetIP, targetPort, key, 0, time.Second*10).Result()
			if err != nil {
				fmt.Printf("迁移slot: %d 中的数据失败: %v\n", slot, err)
				return
			}
			fmt.Printf(".") // 打印迁移 key 进度
		}
		if len(ret.Val()) < count {
			//fmt.Printf("\n")
			break
		}
	}

	// 通告集群slot 已经分配给了目标节点,向集群内所有主节点发送命令: cluster setslot [slot] node [target nodeID]
	for _, addr := range data.Masters {
		rc, err := InitStandConn(addr, password)
		if err != nil {
			fmt.Printf("连接 redis: %s 失败: %v\n", addr, err)
			return
		}

		_, err = rc.Do(context.Background(), "cluster", "setslot", slot, "node", data.AddrToID[targetAddr]).Result()
		if err != nil {
			fmt.Printf("在 redis: %s 上执行命令: cluster setslot 失败, err:%v\n", addr, err)
			return
		}
		rc.Close()
	}

	//fmt.Printf("Slot %d 完成迁移\n", slot)
	fmt.Printf("Slot %d Done\n", slot)
}

/*
1.指定 slot,slot所在源节点,slot 要迁移的目的节点
2.对目标节点发送 cluster setslot [slot] importing [source nodeID]
3.对源节点发送 cluster setslot [slot] migrating [target nodeID]
4.源节点循环执行 cluster getkeysinslot [slot] [count]  --获取 count 个属于 slot 的键
5.源节点执行批量迁移 key 的命令 migrate [target ip] [target port] "" 0 [timeout] keys [keys...]
6.重复执行步骤 4 和 5,直到 slot 的所有数据都迁移到目标节点
7.向集群内所有主节点发送 cluster setslot [slot] node [target nodeID],以通知 slot 已经分配给了目标节点
*/

// SlotMove 迁移 slot
func SlotMove(sourceAddr, targetAddr, password string, slots []int64, count int, data *ClusterInfo) {
	redisWG = &sync.WaitGroup{}
	// 建立到 sourceAddr 的连接
	sourceClient, err := InitStandConn(sourceAddr, password)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 建立到 targetAddr 的连接
	targetClient, err := InitStandConn(targetAddr, password)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 函数结束后关闭 redis 连接
	defer sourceClient.Close()
	defer targetClient.Close()

	fmt.Printf("FROM sourceAddr: %s sourceNodeID: %s\n", sourceAddr, data.AddrToID[sourceAddr])
	fmt.Printf("TO targetAddr: %s targetNodeID: %s\n", targetAddr, data.AddrToID[targetAddr])

	// 迁移 slot
	for _, slot := range slots {
		redisWG.Add(1)
		go move(sourceClient, targetClient, sourceAddr, targetAddr, password, slot, count, data)

	}
	redisWG.Wait()
}
