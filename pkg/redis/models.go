package redis

import (
	"context"
	"time"
)

var (
	ctx = context.Background()
)

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
	SlotStr    string
}

type ClusterNodesMap struct {
	ClusterNodesDetail []*ClusterNode
	Masters            []string
	Slaves             []string
	IDToAddr           map[string]string
	AddrToID           map[string]string
}
