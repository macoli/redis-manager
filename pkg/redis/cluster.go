package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/macoli/redis-manager/pkg/slice"

	"github.com/go-redis/redis/v8"
)

// GetClusterNodes 执行 cluster nodes 命令并格式化结果
func GetClusterNodes(addr, password string) (ClusterNodes []*ClusterNode, err error) {
	// 初始化 redis 连接
	var rc *redis.Client
	rc, err = InitStandRedis(addr, password)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// 执行 cluster nodes 命令
	var ret string
	ret, err = rc.ClusterNodes(ctx).Result()
	if err != nil {
		errMsg := fmt.Sprintf("%s 执行命令: cluster nodes 失败, err:%v\n", addr, err)
		return nil, errors.New(errMsg)
	}
	ret = strings.Trim(ret, "\n") // 去掉首尾的换行符

	// 按换行符切割,并对每行格式化为 ClusterNode
	for _, item := range strings.Split(ret, "\n") {
		node := &ClusterNode{}
		fields := strings.Split(item, " ")
		node.ID = fields[0]
		nodeSlice := strings.Split(fields[1], "@")
		node.Addr = nodeSlice[0]
		if len(nodeSlice) == 2 {
			node.ClusterPort = nodeSlice[1]
		} else {
			node.ClusterPort = ""
		}
		node.Flags = strings.Split(fields[2], ",")
		node.MasterID = fields[3]
		node.PingSent, err = strconv.ParseInt(fields[4], 10, 64)
		if err != nil {
			errMsg := fmt.Sprintf("%s 的 ping-sent 字段 %s 转换成 int64 类型失败, err:%v\n", node.Addr, fields[4], err)
			return nil, errors.New(errMsg)
		}
		node.PongRecv, err = strconv.ParseInt(fields[5], 10, 64)
		if err != nil {
			errMsg := fmt.Sprintf("%s 的 pong-recv 字段 %s 转换成 int64 类型失败, err:%v\n", node.Addr, fields[4], err)
			return nil, errors.New(errMsg)
		}
		node.ConfigEpoch, err = strconv.ParseInt(fields[6], 10, 64)
		if err != nil {
			errMsg := fmt.Sprintf("%s 的 config-epoch 字段 %s 转换成 int64 类型失败 err:%v\n", node.Addr, fields[4], err)
			return nil, errors.New(errMsg)
		}
		node.LinkState = fields[7]
		if len(fields) == 8 {
			node.Slots = nil
		} else {
			node.Slots = fields[8:]
		}

		ClusterNodes = append(ClusterNodes, node)
	}

	return
}

// FormatClusterNodes 获取集群节点信息,生成结构体 ClusterNodesInfoFormat
func FormatClusterNodes(addr, password string) (data *ClusterNodesInfoFormat, err error) {
	var ClusterNodes []*ClusterNode
	var MasterSlaveMaps []*MasterSlaveMap
	var MasterAddrs []string
	var SlaveAddrs []string
	IDToAddr := make(map[string]string)
	AddrToID := make(map[string]string)

	// 获取集群 nodes 信息
	ClusterNodes, err = GetClusterNodes(addr, password)
	if err != nil {
		return nil, err
	}

	// 格式化 ClusterNodes, 生成 ClusterNodesInfoFormat
	NodeTmpMap := map[string]map[string]string{} // 临时存放主从映射关系 {nodeID:{maasterAddr: xx, ...}}
	for _, node := range ClusterNodes {
		if _, ok := slice.Find(node.Flags, "master"); ok { // 角色是 master
			MasterAddrs = append(MasterAddrs, node.Addr)
			IDToAddr[node.ID] = node.Addr
			AddrToID[node.Addr] = node.ID

			var slotStr string
			if node.Slots != nil {
				slotStr = strings.Join(node.Slots, " ")
			} else {
				slotStr = ""
			}

			if _, ok := NodeTmpMap[node.ID]; !ok { // 判断NodeTmpMap[node.ID]是否存在,不存在则创建
				NodeTmpMap[node.ID] = map[string]string{
					"masterAddr": node.Addr,
					"SlotStr":    slotStr,
				}
				continue
			}
			NodeTmpMap[node.ID]["masterAddr"] = node.Addr
			NodeTmpMap[node.ID]["SlotStr"] = slotStr
		}

		if _, ok := slice.Find(node.Flags, "slave"); ok { // 角色是 slave
			SlaveAddrs = append(SlaveAddrs, node.Addr)
			IDToAddr[node.ID] = node.Addr
			AddrToID[node.Addr] = node.ID

			if _, ok := NodeTmpMap[node.MasterID]; !ok { // 判断NodeTmpMap[node.ID]是否存在,不存在则创建
				NodeTmpMap[node.MasterID] = map[string]string{
					"masterAddr": node.Addr,
				}
				continue
			}
			NodeTmpMap[node.MasterID]["masterAddr"] = node.Addr
		}
	}

	// 生成 MasterSlaveMaps
	for masterID, item := range NodeTmpMap {
		node := &MasterSlaveMap{
			masterID,
			item["masterAddr"],
			item["slaveAddr"],
			item["slaveID"],
			item["SlotStr"],
		}
		MasterSlaveMaps = append(MasterSlaveMaps, node)
	}
	data = &ClusterNodesInfoFormat{
		ClusterNodes:    ClusterNodes,
		MasterSlaveMaps: MasterSlaveMaps,
		Masters:         MasterAddrs,
		Slaves:          SlaveAddrs,
		IDToAddr:        IDToAddr,
		AddrToID:        AddrToID,
	}
	return
}

// CheckClusterConfig 校验集群配置项是否一致
func CheckClusterConfig(addrSlice []string, password, configArg string) (bool, error) {
	var retValue string
	for _, addr := range addrSlice {
		// 创建 redis 连接
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return false, err
		}

		// 获取配置项
		argRet, err := rc.ConfigGet(ctx, configArg).Result()
		if err != nil {
			return false, err
		}

		if retValue != argRet[1].(string) && retValue != "" {
			err = errors.New("集群配置项的值不一致")
			return false, err
		} else {
			retValue = argRet[1].(string)
		}
		rc.Close()
	}
	return true, nil
}

// ClusterGetConfig 获取集群配置并校验是否一致
func ClusterGetConfig(addrSlice []string, password, configArg string) (ret string, err error) {
	for _, addr := range addrSlice {
		// 连接 redis
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return "", err
		}

		argRet, err := rc.ConfigGet(ctx, configArg).Result()
		if err != nil {
			errMsg := fmt.Sprintf("获取集群配置项 %s 失败, err:%v\n", configArg, err)
			return "", errors.New(errMsg)
		}
		retValue := argRet[1].(string)
		if ret != argRet[1].(string) && ret != "" {
			err := errors.New("集群配置项的值不一致")
			return "", err
		} else {
			ret = retValue
		}
		rc.Close()
	}
	return
}

// ClusterSetConfig 批量设置集群配置
func ClusterSetConfig(addrSlice []string, password, configArg, setValue string) (err error) {
	for _, addr := range addrSlice {
		// 连接 redis
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}

		err = rc.ConfigSet(ctx, configArg, setValue).Err()
		if err != nil {
			errMsg := fmt.Sprintf("集群节点设置 %s 的值: %s 失败\n", configArg, setValue)
			return errors.New(errMsg)
		}
		rc.Close()
	}
	return
}

// ClusterFlushAll 集群数据清理
func ClusterFlushAll(data *ClusterNodesInfoFormat, password, flushCMD string) (err error) {
	clusterNodes := append(data.Masters, data.Slaves...)
	// 获取cluster-node-timeout配置值
	ret, err := ClusterGetConfig(clusterNodes, password, "cluster-node-timeout")
	if err != nil {
		return err
	}

	for _, addr := range clusterNodes {
		// 连接 redis
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}
		defer rc.Close()

		// 获取 redis 版本
		infoMap, err := FormatRedisInfo(addr, password)
		if err != nil {
			return err
		}
		versionPrefixStr := strings.Split(infoMap["redis_version"], ".")[0]
		versionPrefix, err := strconv.ParseInt(versionPrefixStr, 10, 64)
		if err != nil {
			errMsg := fmt.Sprintf("获取redis: %s 版本失败\n", addr)
			return errors.New(errMsg)
		}

		// 针对不同版本的 redis, 执行不同的的清空操作
		if versionPrefix == 3 { // 清空会堵塞 redis,造成主从切换,需要先调整集群超时时间
			// 调整将cluster-node-timeout配置项的值为 30 分钟,避免清空 redis 的时候发生主从切换
			err = ClusterSetConfig(clusterNodes, password, "cluster-node-timeout", ret)
			if err != nil {
				return err
			}

			//对每个节点执行 FLUSHALL 命令
			if flushCMD == "FLUSHALL" {
				err = rc.FlushAll(ctx).Err()
				if err != nil {
					errMsg := fmt.Sprintf("执行 FLUSHALL 命令失败, err:%v\n", err)
					return errors.New(errMsg)
				}
			} else {
				err = rc.Do(ctx, flushCMD).Err()
				if err != nil {
					errMsg := fmt.Sprintf("执行 FLUSHALL 的 rename 命令: %s 失败, err:%v\n", flushCMD, err)
					return errors.New(errMsg)
				}
			}

			// 将cluster-node-timeout配置修改为原来配置的值
			err = ClusterSetConfig(clusterNodes, password, "cluster-node-timeout", ret)
			if err != nil {
				return err
			}
		} else if versionPrefix >= 4 { // 执行异步清空
			//对每个节点执行 FLUSHALL 命令
			if flushCMD == "FLUSHALL" {
				err = rc.Do(ctx, "FLUSHALL", "ASYNC").Err()
				if err != nil {
					errMsg := fmt.Sprintf("执行 FLUSHALL ASYNC 命令失败, err:%v\n", err)
					return errors.New(errMsg)
				}
			} else {
				err = rc.Do(ctx, flushCMD, "ASYNC").Err()
				if err != nil {
					errMsg := fmt.Sprintf("执行 FLUSHALL 的 rename 命令: %s ASYNC 失败, err:%v\n", flushCMD, err)
					return errors.New(errMsg)
				}
			}
		}

	}
	fmt.Println("集群已清空")
	return
}

// CheckClusterState 检查集群状态
func CheckClusterState(addr, password string) (ret string) {
	// 获取集群所有节点
	data, err := FormatClusterNodes(addr, password)
	if err != nil {
		errMsg := fmt.Sprintf("获取集群节点信息失败,err:%v\n", err)
		ret += errMsg
		return
	}

	// 检查集群 slot 数量是否为 16384
	slotCount := 0
	for _, addr := range data.Masters {
		slots, err := GetClusterAddrSlots(data, addr)
		if err != nil {
			return
		}
		fmt.Printf("%s slots: %d\n", addr, len(slots))
		slotCount += len(slots)
	}
	if slotCount != 16384 {
		errMsg := fmt.Sprintf("集群 slot 总数不是 16384\n")
		ret += errMsg
	} else {
		ret += fmt.Sprintf("集群 slot 总数为 16384\n")
	}

	// 检查集群 slot 是否有处于 migrating 或 importing 状态
	var slotStateErrMsg string
	for _, addr := range data.Masters {
		data, err := GetClusterNodes(addr, password)
		if err != nil {
			errMsg := fmt.Sprintf("通过 %s 获取集群节点信息失败, err:%v\n", addr, err)
			ret += errMsg
			return
		}

		for _, node := range data {
			if _, ok := slice.Find(node.Flags, "myself"); ok {
				// 判断节点是否有 slot migrating 和 importing 转态
				if index, ok := slice.Has(node.Slots, "->-"); ok { // migrating
					errSlot := strings.Split(node.Slots[index], "->-")[0]
					slotStateErrMsg += fmt.Sprintf("节点 %s 的 slot %s 处于 migrating 状态\n", node.Addr, errSlot)
					ret += slotStateErrMsg
				} else if index, ok := slice.Has(node.Slots, "-<-"); ok { // importing
					errSlot := strings.Split(node.Slots[index], "-<-")[0]
					slotStateErrMsg += fmt.Sprintf("节点 %s 的 slot %s 处于 importing 状态\n", node.Addr, errSlot)
					ret += slotStateErrMsg
				}
			}
		}
	}
	if len(slotStateErrMsg) == 0 {
		ret += fmt.Sprintf("集群所有 slot 都没有处于 migrating 或 importing 状态\n")
	}
	fmt.Println(ret)
	return
}
