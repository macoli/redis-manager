package check

/*
分析 iredis
数据搜集:
1.节点分布情况(主机)
2.节点所在主机信息(负载/cpu/内存/磁盘/带宽等)
3.Redis INFO信息

数据分析:
通用:
1.节点所在主机状态判断
	a.负载大于 1.5
	b.内存使用率>=80%
	c.磁盘使用率>=80%
	d.磁盘吞吐>=100MB/s
	e.磁盘 IOPS >=1000
	f.主机网络连通性(ping)
2.节点状态
	a.是否能访问
	b.访问时延
3.主从状态
	a.主从同步 master_link_status
	b.主从进度 offset
	c.主从是否在同一机器(如果是虚拟机,主从所在虚拟机是否在同一物理机)
4.节点内存
	a.内存使用率>=80%
	b.内存碎片
5.节点连接数
	a.连接数超过最大连接数的60%
	b.连接数大于 2000


集群:
1.master 节点数判断,必须为奇数个
2.单台机器上集群的节点数量不能多于 master/2

*/

func Run() {
	//接收参数
	//addr, password, redisType := param.Check()
	//switch redisType {
	//case "standalone":
	//case "sentinel":
	//case "cluster":
	//
	//}

}
