package redis

import "time"

type SlowLog struct {
	Instance string
	Command  string
	Duration time.Duration
	Time     string
}

type ClusterNode struct {
	MasterID   string
	MasterAddr string
	SlaveAddr  string
	SlaveID    string
}

type ClusterNodesMap struct {
	ClusterNodesDetail []*ClusterNode
	Masters            []string
	Slaves             []string
	IDToAddr           map[string]string
	AddrToID           map[string]string
}
