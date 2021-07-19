package showClusterMap

import (
	"fmt"
	"os"
	"sort"
	"strings"

	t "github.com/macoli/redis-manager/pkg/table"

	r "github.com/macoli/redis-manager/pkg/redis"
)

type ClusterMap struct {
	MasterID   string
	MasterAddr string
	SlaveAddr  string
	SlaveID    string
}

//get cluster nodes and format
func formatClusterNodes(addr string) (clusterMap []ClusterMap, err error) {
	//get cluster nodes
	ret, err := r.GetClusterNodes(addr)
	if err != nil {
		return nil, err
	}
	c := map[string]map[string]string{}
	// format the ret, get all instance addr
	clusterNodesSlice := strings.Split(ret, "\n")
	for _, node := range clusterNodesSlice {
		if len(node) == 0 {
			continue
		}
		nodeSlice := strings.Split(node, " ")

		role := nodeSlice[2]
		if strings.Contains(role, "myself") {
			role = strings.Split(role, ",")[1]
		}

		switch role {
		case "master":
			if _, ok := c[nodeSlice[0]]; !ok {
				c[nodeSlice[0]] = map[string]string{
					"masterAddr": strings.Split(nodeSlice[1], "@")[0],
				}
			}
			c[nodeSlice[0]]["masterAddr"] = strings.Split(nodeSlice[1], "@")[0]
		case "slave":
			if _, ok := c[nodeSlice[3]]; !ok {
				c[nodeSlice[3]] = map[string]string{
					"slaveAddr": strings.Split(nodeSlice[1], "@")[0],
					"slaveID":   nodeSlice[0],
				}
			}
			c[nodeSlice[3]]["slaveAddr"] = strings.Split(nodeSlice[1], "@")[0]
			c[nodeSlice[3]]["slaveID"] = nodeSlice[0]
		}
	}

	for mID, value := range c {
		node := ClusterMap{
			mID,
			value["masterAddr"],
			value["slaveAddr"],
			value["slaveID"],
		}
		clusterMap = append(clusterMap, node)
	}
	return
}

// data sort
func dataSort(s string, data []ClusterMap) {
	sort.Slice(data, func(i, j int) bool {
		switch s {
		case "MasterID":
			return data[i].MasterID < data[j].MasterID
		case "MasterIP":
			return data[i].MasterAddr < data[j].MasterAddr
		case "SlaveIP":
			return data[i].SlaveAddr > data[j].SlaveAddr
		case "SlaveID":
			return data[i].SlaveID > data[j].SlaveID
		default:
			return data[i].MasterAddr > data[j].MasterAddr
		}

	})
}

//show the cluster map by table
func show(data []ClusterMap, sortBy string) {
	if len(data) == 0 {
		fmt.Println("this is nil cluster, please check the cluster status!")
		os.Exit(0)
	}
	dataSort(sortBy, data)

	HeaderCells := t.GenHeaderCells(ClusterMap{})

	dataInterface := make([]interface{}, len(data))
	for i, rowMap := range data {
		row := []string{
			rowMap.MasterID,
			rowMap.MasterAddr,
			rowMap.SlaveAddr,
			rowMap.SlaveID,
		}
		dataInterface[i] = row
	}
	BodyCells := t.GenBodyCells(dataInterface)

	t.ShowTable(HeaderCells, BodyCells)
}

// Run show cluster map main
func Run(instance, sortBy string) {
	clusterMap, err := formatClusterNodes(instance)
	if err != nil {
		fmt.Printf("format the cluster nodes info failed, err:%v\n", err)
	}
	show(clusterMap, sortBy)
}
