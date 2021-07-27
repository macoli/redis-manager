package redis

import (
	"fmt"
	"strings"
	"time"
)

//SetSlotImporting 对目标节点执行 cluster setslot [slot] importing [source nodeID]
func SetSlotImporting(targetAddr string, password string, slot int, sourceNodeID string) (err error) {
	rc, err := InitStandRedis(targetAddr, password)
	if err != nil {
		return
	}
	defer rc.Close()
	_, err = rc.Do(CTX, "cluster", "setslot", slot, "importing", sourceNodeID).Result()
	if err != nil {
		return
	}
	return nil
}

//SetSlotMigrating 对源节点发送 cluster setslot [slot] migrating [target nodeID]
func SetSlotMigrating(sourceAddr string, password string, slot int, targetNodeID string) (err error) {
	rc, err := InitStandRedis(sourceAddr, password)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = rc.Do(CTX, "cluster", "setslot", slot, "migrating", targetNodeID).Result()
	if err != nil {
		return err
	}
	return nil
}

//MoveData 迁移 slot 数据
func MoveData(sourceAddr string, targetAddr string, password string, slot int, count int) (err error) {
	//连接源节点
	rc, err := InitStandRedis(sourceAddr, password)
	if err != nil {
		return err
	}
	defer rc.Close()

	//获取目标节点的 ip 和 port
	targetIP := strings.Split(targetAddr, ":")[0]
	targetPort := strings.Split(targetAddr, ":")[1]

	//循环迁移 slot 的数据到目标节点
	for {
		ret := rc.ClusterGetKeysInSlot(CTX, slot, count)
		//fmt.Printf("%#v\n", ret)
		for _, key := range ret.Val() {
			_, err := rc.Migrate(CTX, targetIP, targetPort, key, 0, time.Second*10).Result()
			if err != nil {
				return err
			}
			//fmt.Printf("migrate result:%v\n", ret)
			fmt.Printf("#")
		}
		if len(ret.Val()) < count {
			fmt.Printf("\n")
			break
		}
	}
	return nil
}

//SetSlotNode 向集群内所有主节点发送 cluster setslot [slot] node [target nodeID],以通知 slot 已经分配给了目标节点
func SetSlotNode(clusterNodes []string, password string, slot int, targetNodeID string) (err error) {
	for _, addr := range clusterNodes {
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}

		_, err = rc.Do(CTX, "cluster", "setslot", slot, "node", targetNodeID).Result()
		if err != nil {
			return err
		}
	}
	return nil
}
