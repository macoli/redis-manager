package iredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/macoli/redis-manager/pkg/islice"
)

// ========================================cluster info format==========================================

type ClusterNode struct {
	ID          string   // 当前节点 ID
	Addr        string   // 当前节点地址(ip:port)
	ClusterPort string   // 当前节点和集群其他节点通信端口(默认为节点端口+1000),3.x 版本不展示该信息
	Flags       []string // 当前节点标志:myself, master, slave, fail?, fail, handshake, noaddr, nofailover, noflags
	MasterID    string   // 如果当前节点是 slave,这里就是 对应 master 的 ID,如果当前节点是 master,以"-"表示
	PingSent    int64    // 最近一次发送ping的时间，这个时间是一个unix毫秒时间戳，0代表没有发送过
	PongRecv    int64    // 最近一次收到pong的时间，使用unix时间戳表示
	ConfigEpoch int64    // 节点的epoch值.每当节点发生失败切换时，都会创建一个新的，独特的，递增的epoch。如果多个节点竞争同一个哈希槽时，epoch值更高的节点会抢夺到。
	LinkState   string   // node-to-node集群总线使用的链接的状态: connected或disconnected
	Slots       []string // 哈希槽值或者一个哈希槽范围
}

// getNodes 格式化 cluster nodes 命令返回的结果
// GetNodes 函数用于解析nodesStr字符串，将其转换为[]*ClusterNode切片，并返回
func getNodes(nodesStr string) (nodes []*ClusterNode, err error) {
	nodesStr = strings.Trim(nodesStr, "\n") // 去掉首尾的换行符

	// 按换行符切割,并对每行格式化为 ClusterNode
	for _, item := range strings.Split(nodesStr, "\n") {
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

		nodes = append(nodes, node)
	}

	return
}

type MasterSlaveMap struct {
	MasterID   string
	MasterAddr string
	SlaveAddr  string
	SlaveID    string
	SlotStr    string
}

type ClusterInfo struct {
	ClusterNodes    []*ClusterNode
	MasterSlaveMaps []*MasterSlaveMap
	Masters         []string
	Slaves          []string
	IDToAddr        map[string]string
	AddrToID        map[string]string
}

// ClusterInfoFormat 通过cluster nodes 命令返回的结果,格式化为自定义的结构体数据 ClusterInfo
func ClusterInfoFormat(addr, password string) (data *ClusterInfo, err error) {
	// 获取 nodeStr
	rc, err := InitStandConn(addr, password)
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	nodeStr, err := rc.ClusterNodes(ctx).Result()
	cancel()
	rc.Close()

	var MasterSlaveMaps []*MasterSlaveMap
	var MasterAddrs []string
	var SlaveAddrs []string
	IDToAddr := make(map[string]string)
	AddrToID := make(map[string]string)

	// 获取集群 nodes 信息
	ClusterNodes, err := getNodes(nodeStr)
	if err != nil {
		return nil, err
	}

	// 格式化 ClusterNodes, 生成 ClusterInfo
	NodeTmpMap := map[string]map[string]string{} // 临时存放主从映射关系 {nodeID:{maasterAddr: xx, ...}}
	for _, node := range ClusterNodes {
		if _, ok := islice.Find(node.Flags, "master"); ok { // 角色是 master
			MasterAddrs = append(MasterAddrs, node.Addr)
			IDToAddr[node.ID] = node.Addr
			AddrToID[node.Addr] = node.ID

			var slotStr string
			if node.Slots != nil {
				slotStr = strings.Join(node.Slots, ",")
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

		if _, ok := islice.Find(node.Flags, "slave"); ok { // 角色是 slave
			SlaveAddrs = append(SlaveAddrs, node.Addr)
			IDToAddr[node.ID] = node.Addr
			AddrToID[node.Addr] = node.ID

			if _, ok := NodeTmpMap[node.MasterID]; !ok { // 判断NodeTmpMap[node.ID]是否存在,不存在则创建
				NodeTmpMap[node.MasterID] = map[string]string{
					"slaveAddr": node.Addr,
					"slaveID":   node.ID,
				}
				continue
			}
			NodeTmpMap[node.MasterID]["slaveAddr"] = node.Addr
			NodeTmpMap[node.MasterID]["slaveID"] = node.ID
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
	data = &ClusterInfo{
		ClusterNodes:    ClusterNodes,
		MasterSlaveMaps: MasterSlaveMaps,
		Masters:         MasterAddrs,
		Slaves:          SlaveAddrs,
		IDToAddr:        IDToAddr,
		AddrToID:        AddrToID,
	}
	return
}

// =================================cluster check=================================================

// clusterCheckConfig 校验集群配置项是否一致
func clusterCheckConfig(addrSlice []string, password, item string) (itemValue string, err error) {
	for _, addr := range addrSlice {
		// 连接 iredis
		rc, err := InitStandConn(addr, password)
		if err != nil {
			return "", err
		}

		argRet, err := rc.ConfigGet(context.Background(), item).Result()
		if err != nil {
			errMsg := fmt.Sprintf("获取集群配置项 %s 失败, err:%v\n", item, err)
			return "", errors.New(errMsg)
		}
		retValue := argRet[1].(string)
		if itemValue != argRet[1].(string) && itemValue != "" {
			err := errors.New("集群配置项的值不一致")
			return "", err
		} else {
			itemValue = retValue
		}
		rc.Close()
	}
	return
}

// clusterCheckVersion 校验集群版本是否一致
func clusterCheckVersion(addrSlice []string, password string) (version string, err error) {
	for _, addr := range addrSlice {
		// 创建 iredis 连接
		rc, err := InitStandConn(addr, password)
		if err != nil {
			return "", err
		}

		// 获取 iredis 版本
		infoStr, err := rc.Info(context.Background()).Result()
		if err != nil {
			errMsg := fmt.Sprintf("获取redis %s 的 Info 信息失败--%v", addr, err)
			return "", errors.New(errMsg)
		}
		infoMap, err := InfoMap(infoStr)
		if err != nil {
			return "", err
		}

		if version != "" && version != infoMap["redis_version"] {
			errMsg := fmt.Sprintf("集群的 iredis 版本不一致")
			return "", errors.New(errMsg)
		} else {
			version = infoMap["redis_version"]
		}

		rc.Close()
	}
	return
}

// =================================cluster config=================================================
// ClusterConfigGet 获取集群配置并校验是否一致
func ClusterConfigGet(addrSlice []string, password, item string) (ret string, err error) {
	return clusterCheckConfig(addrSlice, password, item)
}

// ClusterConfigSet 批量设置集群配置
func ClusterConfigSet(addrSlice []string, password, configKey, setValue string) (err error) {
	// 校验集群配置是否一致
	_, err = ClusterConfigGet(addrSlice, password, configKey)
	if err != nil {
		return err
	}

	// 批量修改配置
	for _, addr := range addrSlice {
		// 连接 iredis
		rc, err := InitStandConn(addr, password)
		if err != nil {
			return err
		}

		err = rc.ConfigSet(context.Background(), configKey, setValue).Err()
		if err != nil {
			errMsg := fmt.Sprintf("集群节点设置 %s 的值: %s 失败\n", configKey, setValue)
			return errors.New(errMsg)
		}

		rc.Close()
	}
	return
}

// ==================================cluster flush==================================================

// ClusterFlush 清空整个集群所有节点的数据
func ClusterFlush(data *ClusterInfo, password, flushCMD string, workerNums int) {
	var wg sync.WaitGroup
	// 设置最大并发数量
	var workerChannel chan struct{}
	if workerNums == 0 {
		workerChannel = make(chan struct{}, 1)
	} else {
		workerChannel = make(chan struct{}, workerNums)
	}

	clusterNodes := append(data.Masters, data.Slaves...)

	// 获取集群版本
	version, err := clusterCheckVersion(clusterNodes, password)
	if err != nil {
		fmt.Println(err)
		return
	}
	versionPrefixStr := strings.Split(version, ".")[0]
	versionPrefix, err := strconv.Atoi(versionPrefixStr)
	if err != nil {
		fmt.Printf("集群版本 %s 的大版本号转换成数字失败\n", versionPrefixStr)
		return
	}

	if versionPrefix == 3 { // iredis 版本为 3.x
		// 获取cluster-node-timeout配置值(保存,后续恢复时使用)
		fmt.Printf("集群当前 iredis 版本为: %s, 需要调大配置项 cluster-node-timeout 的值,避免清理过程中发生主从切换\n", version)

		ret, err := ClusterConfigGet(clusterNodes, password, "cluster-node-timeout")
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("集群配置项 cluster-node-timeout 的值当前为: %s (ms), 准备调大到 1800000 (ms, 30min)\n", ret)

		// 调整将cluster-node-timeout配置项的值为 30 分钟,避免清空 iredis 的时候发生主从切换
		err = ClusterConfigSet(clusterNodes, password, "cluster-node-timeout", "1800000")
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("集群配置项 cluster-node-timeout 的值已调整为 1800000\n")

		// 并发清空 iredis 操作
		for _, addr := range data.Masters {
			workerChannel <- struct{}{} // 添加信号,当 workerChannel 满了之后就会阻塞创建新的 goroutine
			wg.Add(1)
			go func(address string) {
				defer wg.Done()
				// 执行完毕,释放信号
				defer func() {
					<-workerChannel
				}()

				// =================清空 iredis =========================
				// 连接 iredis
				rc := redis.NewClient(&redis.Options{
					Addr:        address,
					Password:    password,
					DB:          0,
					PoolSize:    100,
					DialTimeout: time.Minute * 30,
					ReadTimeout: time.Minute * 30,
				})

				_, err := rc.Ping(context.Background()).Result()
				if err != nil {
					fmt.Printf("iredis 实例 %s 连接失败: %v\n", addr, err)
					return
				}

				defer rc.Close()

				fmt.Printf("开始清空 %s 数据\n", addr)
				switch flushCMD {
				case "FLUSHALL":
					err = rc.Do(context.Background(), "FLUSHALL").Err()
					if err != nil {
						fmt.Printf("***** %s 执行 FLUSHALL 命令失败: %v\n", addr, err)
						return
					}
				default:
					err = rc.Do(context.Background(), flushCMD).Err()
					if err != nil {
						fmt.Printf("***** %s 执行 FLUSHALL 的 rename 命令 %s 失败: %v\n", addr, flushCMD, err)
						return
					}

				}
				fmt.Printf("!!! %s 数据已清空\n", addr)
			}(addr)
		}

		wg.Wait()
		fmt.Printf("集群已清理完成, 将集群配置项 cluster-node-timeout 的值还原为 %s\n", ret)
		cnt := 0
		for {
			if cnt >= 5 {
				fmt.Printf("还原集群配置项 cluster-node-timeout 失败,请检查集群状态,并将其手动还原为: %s\n", ret)
				return
			}
			// 将cluster-node-timeout配置修改为原来配置的值
			err = ClusterConfigSet(clusterNodes, password, "cluster-node-timeout", ret)
			if err != nil {
				cnt += 1
				interval := time.Second * time.Duration(cnt*10)
				fmt.Printf("还原配置项失败, %v 后重试第 %d 次\n", interval, cnt)
				time.Sleep(interval)
				continue
			} else {
				ret1, err := ClusterConfigGet(clusterNodes, password, "cluster-node-timeout")
				if err != nil {
					fmt.Printf("还原配置项 cluster-node-timeout 成功,但获取其当前值时失败: %v\n", err)
					return
				}
				fmt.Printf("还原配置项 cluster-node-timeout 成功, 其当前值为:%s\n", ret1)
				return
			}

		}

	} else if versionPrefix >= 4 { // iredis 版本为 4.x 版本及以上
		fmt.Printf("集群当前 iredis 版本为: %s, 支持异步清空数据,清空命令执行完后会在后台逐步完成清理\n", version)
		// 并发清空 iredis 操作
		for _, addr := range data.Masters {
			workerChannel <- struct{}{} // 添加信号,当 workerChannel 满了之后就会阻塞创建新的 goroutine
			wg.Add(1)
			go func(address string) {
				defer wg.Done()
				// 执行完毕,释放信号
				defer func() {
					<-workerChannel
				}()

				// =================清空 iredis =========================
				// 连接 iredis
				rc, err := InitStandConn(address, password)
				if err != nil {
					fmt.Println(err)
				}
				defer rc.Close()

				fmt.Printf("开始对 %s 异步清空数据\n", addr)
				switch flushCMD {
				case "FLUSHALL":
					err = rc.Do(context.Background(), "FLUSHALL", "ASYNC").Err()
					if err != nil {
						fmt.Printf("***** %s 执行 FLUSHALL ASYNC 命令失败: %v\n", addr, err)
						return
					}
				default:
					err = rc.Do(context.Background(), flushCMD, "ASYNC").Err()
					if err != nil {
						fmt.Printf("***** %s 执行 FLUSHALL 的 rename 命令 %s ASYNC 失败: %v\n", addr, flushCMD, err)
						return
					}

				}
				fmt.Printf("!!! %s 异步清空命令已执行\n", addr)
			}(addr)
		}
		wg.Wait()
	}

}
