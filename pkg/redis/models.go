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

type MasterSlaveMap struct {
	MasterID   string
	MasterAddr string
	SlaveAddr  string
	SlaveID    string
	SlotStr    string
}

type ClusterNodesInfoFormat struct {
	ClusterNodes    []*ClusterNode
	MasterSlaveMaps []*MasterSlaveMap
	Masters         []string
	Slaves          []string
	IDToAddr        map[string]string
	AddrToID        map[string]string
}
