# redis-manager

redis 运维工具集

## 编译

```
go buid main.go
```


## 用法

```
./redis-manager
Usage of redis-manager:
Options:
        slowlog         慢查询信息展示
        moveslot        集群迁移指定的slot到指定节点(仅支持迁移单个slot)
        clustermap      集群映射关系展示
        clusterclear    集群数据清空
        clusterconfig   集群配置相关:获取配置项,批量修改配置项
```

## 功能

- [x] 慢查询信息展示
- [x] 集群主从映射关系展示
- [x] 迁移指定 slot 到指定节点
- [ ] redis 状态检查（节点分布、节点状态、主机状态等）
- [x] redis 清空（依赖命令 FLUSHALL，支持使用 rename 后的命令）
- [ ] 集群指定实例（批量）切换主从
- [x] 集群配置（获取集群配置项、批量修改配置）