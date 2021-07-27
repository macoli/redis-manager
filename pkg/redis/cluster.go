package redis

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
)

//GetClusterNodes 获取集群 info 信息
func GetClusterNodes(addr string, password string) (ret string, err error) {
	// init showSlowLog cluster conn
	var rc *redis.ClusterClient
	rc, err = InitClusterRedis(addr, password)
	defer rc.Close()

	// showSlowLog command: cluster nodes
	ret, err = rc.ClusterNodes(CTX).Result()
	if err != nil {
		return "", err
	}
	return
}

//FormatClusterNodes 获取集群节点信息,并按 master/slave 进行分类:{"master":[], "slave":[]}
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

		role := nodeSlice[2]
		if strings.Contains(role, "myself") {
			role = strings.Split(role, ",")[1]
		}

		if role == "master" {
			MasterInstances = append(MasterInstances, Addr)
			if _, ok := NodeTmpMap[nodeSlice[0]]; !ok {
				NodeTmpMap[nodeSlice[0]] = map[string]string{
					"masterAddr": Addr,
				}
				continue
			}
			NodeTmpMap[nodeSlice[0]]["masterAddr"] = Addr
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

//ClusterGetConfig 获取集群配置并校验是否一致
func ClusterGetConfig(addrSlice []string, password string, configArg string) (ret string, err error) {
	for _, addr := range addrSlice {
		// init showSlowLog conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return "", err
		}

		argRet, err := rc.ConfigGet(CTX, configArg).Result()
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

//ClusterSetConfig 批量设置集群配置
func ClusterSetConfig(addrSlice []string, password string, configArg string, setValue string) (err error) {
	for _, addr := range addrSlice {
		// init showSlowLog conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}

		err = rc.ConfigSet(CTX, configArg, setValue).Err()
		if err != nil {
			return err
		}
		rc.Close()
	}
	return
}

//ClusterFlushAll 集群数据清理
func ClusterFlushAll(ClusterNodesSlice []string, password string, flushCMD string) (err error) {
	for _, addr := range ClusterNodesSlice {
		// init showSlowLog conn
		rc, err := InitStandRedis(addr, password)
		if err != nil {
			return err
		}
		defer rc.Close()

		//获取集群所有节点参数 cluster-node-timeout 值
		ret, err := rc.ConfigGet(CTX, "cluster-node-timeout").Result()
		if err != nil {
			return err
		}
		clusterTimeoutValue := ret[1].(string)

		//重设参数 cluster-node-timeout 值,防止在执行 FLUSHALL 命令时发生主从切换(10分钟)
		err = rc.ConfigSet(CTX, "cluster-node-timeout", "600000").Err()
		if err != nil {
			return err
		}

		//对每个节点执行 FLUSHALL 命令
		if flushCMD == "FLUSHALL" {
			err = rc.FlushAll(CTX).Err()
			if err != nil {
				return err
			}
		} else {
			err = rc.Do(CTX, flushCMD).Err()
			return err
		}

		//将参数 cluster-node-timeout 还原
		err = rc.ConfigSet(CTX, "cluster-node-timeout", clusterTimeoutValue).Err()
		if err != nil {
			return err
		}
	}
	return
}
