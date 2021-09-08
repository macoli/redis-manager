package redis

import (
	"strings"
)

// InfoMap 格式化 redis 的 info 命令返回的字符串信息为 map[string]string
func InfoMap(info string) (infoMap map[string]string, err error) {
	infoMap = make(map[string]string)
	// 按行分割字符串
	infoSliceTMP := strings.Split(info, "\r\n")

	// 去掉以#开头的和空串
	infoSlice := make([]string, 0)
	for _, item := range infoSliceTMP {
		if strings.HasPrefix(item, "#") || len(item) == 0 {
			continue
		}
		infoSlice = append(infoSlice, item)
	}

	// 格式化 info 字符串信息为 map 格式
	for _, item := range infoSlice {
		itemSlice := strings.Split(item, ":")
		key := itemSlice[0]
		value := itemSlice[1]
		infoMap[key] = value
	}

	return
}
