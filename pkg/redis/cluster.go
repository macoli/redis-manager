package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/macoli/redis-manager/pkg/slice"
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
		if _, ok := slice.Find(node.Flags, "master"); ok { // 角色是 master
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

		if _, ok := slice.Find(node.Flags, "slave"); ok { // 角色是 slave
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
		// 连接 redis
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
		// 创建 redis 连接
		rc, err := InitStandConn(addr, password)
		if err != nil {
			return "", err
		}

		// 获取 redis 版本
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
			errMsg := fmt.Sprintf("集群的 redis 版本不一致")
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
		// 连接 redis
		rc, err := InitStandConn(addr, password)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = rc.ConfigSet(ctx, configKey, setValue).Err()
		if err != nil {
			errMsg := fmt.Sprintf("集群节点设置 %s 的值: %s 失败\n", configKey, setValue)
			return errors.New(errMsg)
		}

		cancel()
		rc.Close()
	}
	return
}

// ==================================cluster flush==================================================
// ClusterFlush 清空整个集群所有节点的数据
func ClusterFlush(data *ClusterInfo, password, flushCMD string, flushWorker int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushAddrChannel chan string
	if flushWorker == 0 {
		flushAddrChannel = make(chan string, 1)
	} else {
		flushAddrChannel = make(chan string, flushWorker)
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

	if versionPrefix == 3 { // redis 版本为 3.x
		// 获取cluster-node-timeout配置值(保存,后续恢复时使用)
		ret, err := ClusterConfigGet(clusterNodes, password, "cluster-node-timeout")
		if err != nil {
			fmt.Println(err)
			return
		}

		// 调整将cluster-node-timeout配置项的值为 30 分钟,避免清空 redis 的时候发生主从切换
		err = ClusterConfigSet(clusterNodes, password, "cluster-node-timeout", "1800")
		if err != nil {
			fmt.Println(err)
			return
		}

		// 同时启动 flushWorker 个并发清理redis
		for i := 0; i < flushWorker; i++ {
			go func() {
			Loop:
				for {
					select {
					case <-ctx.Done():
						break
					case addr := <-flushAddrChannel:
						// 连接 redis
						rc, err := InitStandConn(addr, password)
						if err != nil {
							fmt.Println(err)
						}

						fmt.Printf("开始清空 %s 数据\n", addr)
						switch flushCMD {
						case "FLUSHALL":
							err = rc.Do(context.Background(), "FLUSHALL").Err()
							if err != nil {
								fmt.Printf("***** %s 执行 FLUSHALL 命令失败: %v\n", addr, err)
								rc.Close()
								continue Loop
							}
						default:
							err = rc.Do(context.Background(), flushCMD).Err()
							if err != nil {
								fmt.Printf("***** %s 执行 FLUSHALL 的 rename 命令 %s 失败: %v\n", addr, flushCMD, err)
								rc.Close()
								continue Loop
							}

						}
						fmt.Printf("!!! %s 数据已清空\n", addr)
						rc.Close()
					}
				}
			}()
		}

		// 将 redis 信息发送到 flushAddrChannel(channel 满了会阻塞)
		for _, addr := range data.Masters {
			flushAddrChannel <- addr
		}

		// 当所有数据都发送完后,循环检测 channel 剩余长度, 长度为 0 表示所有数据都处理完成
		for {
			if len(flushAddrChannel) == 0 {
				break
			}
			time.Sleep(time.Second * 1)
		}

		// 将cluster-node-timeout配置修改为原来配置的值
		err = ClusterConfigSet(clusterNodes, password, "cluster-node-timeout", ret)
		if err != nil {
			fmt.Println(err)
			return
		}

	} else if versionPrefix >= 4 { // redis 版本为 4.x 版本及以上
		// 同时启动 flushWorker 个并发清理redis
		for i := 0; i < flushWorker; i++ {
			go func() {
			Loop:
				for {
					select {
					case <-ctx.Done():
						break
					case addr := <-flushAddrChannel:
						// 连接 redis
						rc, err := InitStandConn(addr, password)
						if err != nil {
							fmt.Println(err)
						}

						fmt.Printf("开始清空 %s 数据\n", addr)
						switch flushCMD {
						case "FLUSHALL":
							err = rc.Do(context.Background(), "FLUSHALL", "ASYNC").Err()
							if err != nil {
								fmt.Printf("***** %s 执行 FLUSHALL ASYNC 命令失败: %v\n", addr, err)
								rc.Close()
								continue Loop
							}
						default:
							err = rc.Do(context.Background(), flushCMD, "ASYNC").Err()
							if err != nil {
								fmt.Printf("***** %s 执行 FLUSHALL 的 rename 命令 %s ASYNC 失败: %v\n", addr, flushCMD, err)
								rc.Close()
								continue Loop
							}

						}
						fmt.Printf("!!! %s 数据已清空\n", addr)
						rc.Close()
					}
				}
			}()
		}

		// 将 redis 信息发送到 flushAddrChannel(channel 满了会阻塞)
		for _, addr := range data.Masters {
			flushAddrChannel <- addr
		}

		// 当所有数据都发送完后,循环检测 channel 剩余长度, 长度为 0 表示所有数据都处理完成
		for {
			if len(flushAddrChannel) == 0 {
				break
			}
			time.Sleep(time.Second * 1)
		}
	}
}
