package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

// GetClusterNodes 获取集群 info 信息
func GetClusterNodes(addr string, password string) (ret string, err error) {
	// init showSlowLog cluster conn
	var rc *redis.ClusterClient
	rc, err = InitClusterRedis([]string{addr}, password)
	defer rc.Close()

	// showSlowLog command: cluster nodes
	ret, err = rc.ClusterNodes(ctx).Result()
	if err != nil {
		return "", err
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

// FormatClusterNodes 获取集群节点信息,并按 master/slave 进行分类:{"master":[], "slave":[]}
func FormatClusterNodes(addr string, password string) (data *ClusterNodesMap, err error) {
	var ClusterNodesDetail []*ClusterNode
	var MasterInstances []string
	var SlaveInstances []string
	IDToAddr := make(map[string]string)
	AddrToID := make(map[string]string)

	//获取集群原始的字符串格式的node信息
	var nodeStr string
	nodeStr, err = GetClusterNodes(addr, password)
	if err != nil {
		return
	}
	//格式化上面的字符串结果
	NodeTmpMap := map[string]map[string]string{}
	clusterNodesSlice := strings.Split(nodeStr, "\n")
	for _, node := range clusterNodesSlice {
		if len(node) == 0 {
			continue
		}
		nodeSlice := strings.Split(node, " ")

		ID := nodeSlice[0]
		Addr := strings.Split(nodeSlice[1], "@")[0]
		IDToAddr[ID] = Addr
		AddrToID[Addr] = ID
		SlotStr := strings.Split(node, "connected")[1]

		role := nodeSlice[2]
		if strings.Contains(role, "myself") {
			role = strings.Split(role, ",")[1]
		}

		if role == "master" {
			MasterInstances = append(MasterInstances, Addr)
			if _, ok := NodeTmpMap[nodeSlice[0]]; !ok {
				NodeTmpMap[nodeSlice[0]] = map[string]string{
					"masterAddr": Addr,
					"SlotStr":    strings.Trim(SlotStr, " "),
				}
				continue
			}
			NodeTmpMap[nodeSlice[0]]["masterAddr"] = Addr
			NodeTmpMap[nodeSlice[0]]["SlotStr"] = strings.Trim(SlotStr, " ")
		} else if role == "slave" {
			SlaveInstances = append(SlaveInstances, Addr)
			if _, ok := NodeTmpMap[nodeSlice[3]]; !ok {
				NodeTmpMap[nodeSlice[3]] = map[string]string{
					"slaveAddr": Addr,
					"slaveID":   ID,
				}
				continue
			}
			NodeTmpMap[nodeSlice[3]]["slaveAddr"] = Addr
			NodeTmpMap[nodeSlice[3]]["slaveID"] = ID
		} else {
			msg := fmt.Sprintf("格式化失败,有未识别的角色类型,错误节点信息: %s", nodeSlice)
			err = errors.New(msg)
			return
		}
	}

	for masterID, item := range NodeTmpMap {
		node := &ClusterNode{
			masterID,
			item["masterAddr"],
			item["slaveAddr"],
			item["slaveID"],
			item["SlotStr"],
		}
		ClusterNodesDetail = append(ClusterNodesDetail, node)
	}
	data = &ClusterNodesMap{
		ClusterNodesDetail: ClusterNodesDetail,
		Masters:            MasterInstances,
		Slaves:             SlaveInstances,
		IDToAddr:           IDToAddr,
		AddrToID:           AddrToID,
	}
	return
}

// ClusterGetConfig 获取集群配置并校验是否一致
func ClusterGetConfig(addrSlice []string, password string, configArg string) (ret string, err error) {
	for _, addr := range addrSlice {
		// init showSlowLog conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return "", err
		}

		argRet, err := rc.ConfigGet(ctx, configArg).Result()
		if err != nil {
			return "", err
		}
		retValue := argRet[1].(string)
		if ret != argRet[1].(string) && ret != "" {
			err = errors.New("集群配置项的值不一致")
			return "", err
		} else {
			ret = retValue
		}
		rc.Close()
	}
	return
}

// ClusterSetConfig 批量设置集群配置
func ClusterSetConfig(addrSlice []string, password string, configArg string, setValue string) (err error) {
	for _, addr := range addrSlice {
		// init showSlowLog conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}

		err = rc.ConfigSet(ctx, configArg, setValue).Err()
		if err != nil {
			return err
		}
		rc.Close()
	}
	return
}

// ClusterFlushAll 集群数据清理
func ClusterFlushAll(data *ClusterNodesMap, password string, flushCMD string) (err error) {
	clusterNodes := append(data.Masters, data.Slaves...)
	// 获取cluster-node-timeout配置值
	ret, err := ClusterGetConfig(clusterNodes, password, "cluster-node-timeout")
	if err != nil {
		fmt.Printf("获取集群配置项cluster-node-timeout失败, err:%v\n", err)
		return
	}

	for _, addr := range clusterNodes {
		// init showSlowLog conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}
		defer rc.Close()

		// 获取 redis 版本
		infoMap, err := FormatRedisInfo(addr, password)
		versionPrefixStr := strings.Split(infoMap["redis_version"], ".")[0]
		versionPrefix, err := strconv.ParseInt(versionPrefixStr, 10, 64)
		if err != nil {
			fmt.Printf("获取redis: %s 版本失败\n", addr)
			return
		}

		// 针对不同版本的 redis, 执行不同的的清空操作
		if versionPrefix == 3 { // 清空会堵塞 redis,造成主从切换,需要先调整集群超时时间
			// 调整将cluster-node-timeout配置项的值为 30 分钟,避免清空 redis 的时候发生主从切换
			err = ClusterSetConfig(clusterNodes, password, "cluster-node-timeout", ret)
			if err != nil {
				fmt.Printf("还原集群配置项cluster-node-timeout失败,配置项初始值为%s, err:%v\n", ret, err)
				return
			}

			//对每个节点执行 FLUSHALL 命令
			if flushCMD == "FLUSHALL" {
				err = rc.FlushAll(ctx).Err()
				if err != nil {
					return err
				}
			} else {
				err = rc.Do(ctx, flushCMD).Err()
				return err
			}

			// 将cluster-node-timeout配置修改为原来配置的值
			err = ClusterSetConfig(clusterNodes, password, "cluster-node-timeout", ret)
			if err != nil {
				fmt.Printf("还原集群配置项cluster-node-timeout失败,配置项初始值为%s, err:%v\n", ret, err)
				return
			}
		} else if versionPrefix >= 4 { // 执行异步清空

		}

	}
	return
}
