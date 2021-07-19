# redis-manager

把一些日常维护使用的功能聚合在一起

## 编译

```
go buid main.go
```


## 用法

```
./redis-manager
Usage of redis-manager:
Options:
  slowlog redis慢查询信息展示
  clustermap redis cluster集群展示
```

## 功能

- [x] 慢查询信息展示
- [x] 集群主从映射关系展示
- [ ] 迁移指定 slot 到指定节点
- [ ] redis 状态检查（节点分布、主机状态、redis状态等）
- [ ] redis 清空（依赖命令 FLUSHALL）
- [ ] 集群指定实例（批量）切换主从