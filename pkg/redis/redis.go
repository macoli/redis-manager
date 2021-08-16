package redis

import (
	"fmt"
	"strings"
)

// FormatRedisInfo 格式化 redis info命令返回的字符串信息
func FormatRedisInfo(addr, password string) (infoMap map[string]string, err error) {
	// 初始化 map
	infoMap = make(map[string]string)

	// 创建 redis 连接
	rc, err := InitStandRedis(addr, password)
	if err != nil {
		fmt.Printf("连接 redis: %s 失败, err:%v\n", addr, err)
		return
	}

	ret, err := rc.Info(ctx).Result()
	if err != nil {
		fmt.Printf("获取 redis: %s 的 info 信息失败, err:%v", addr, err)
		return
	}

	// 按行分割字符串
	infoSliceTMP := strings.Split(ret, "\r\n")

	// 去掉以#开头的和空串
	infoSlice := make([]string, 0)
	for _, item := range infoSliceTMP {
		if strings.HasPrefix(item, "#") || len(item) == 0 {
			continue
		}
		infoSlice = append(infoSlice, item)
	}

	// 格式化
	for _, item := range infoSlice {
		itemSlice := strings.Split(item, ":")
		key := itemSlice[0]
		value := itemSlice[1]
		infoMap[key] = value
	}

	return
}
