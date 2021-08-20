package redis

import (
	"errors"
	"fmt"
	"strings"
)

// FormatSlowLog 获取 redis 慢查询并格式化
func FormatSlowLog(addr string, password string) ([]SlowLog, error) {
	// 创建 redis 连接
	rc, err := InitStandRedis(addr, password)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// 获取 redis 慢查询数量
	nums, err := rc.Do(ctx, "slowlog", "len").Result()
	if err != nil {
		errMsg := fmt.Sprintf("获取 redis 实例: %s 的慢查询数量失败, err:%v\n", addr, err)
		return nil, errors.New(errMsg)
	}

	// 获取 redis 的所有慢查询日志
	ret, err := rc.SlowLogGet(ctx, nums.(int64)).Result()
	if err != nil {
		errMsg := fmt.Sprintf("获取 redis 实例: %s 的慢查询日志失败, err:%v\n", addr, err)
		return nil, errors.New(errMsg)
	}

	// 格式化获取到的慢查询日志
	var data []SlowLog
	for _, item := range ret {
		tmp := SlowLog{
			addr,
			strings.Join(item.Args, " "),
			item.Duration,
			item.Time.String(),
		}
		data = append(data, tmp)
	}
	return data, err
}
