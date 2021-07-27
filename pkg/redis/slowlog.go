package redis

import (
	"strings"

	"github.com/go-redis/redis/v8"
)

//GetSlowLog get slow log info
func GetSlowLog(addr string, password string) (ret []redis.SlowLog, err error) {
	// init showSlowLog conn
	rc, err := InitStandRedis(addr, password)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// get slow log numbers
	nums, err := rc.Do(CTX, "slowlog", "len").Result()
	if err != nil {
		return nil, err
	}

	// get slow log info
	ret, err = rc.SlowLogGet(CTX, nums.(int64)).Result()
	if err != nil {
		return nil, err
	}
	return
}

//FormatSlowLog get slow log and format
func FormatSlowLog(addr string, password string) ([]SlowLog, error) {
	//get slow log
	ret, err := GetSlowLog(addr, password)
	if err != nil {
		return nil, err
	}
	// format slow log
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
