package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/macoli/redis-manager/cmd/showSlowLog"
)

// redis manager project

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, `Usage of redis-manager:
  slowlog: string 展示 redis 慢查询信息
`)
		os.Exit(1)
	}

	//showSlowLog.Run("172.17.10.17:16418", "standalone", "Time")

	// get args from cmd
	ss := flag.NewFlagSet("slowlog", flag.ExitOnError)
	addr := ss.String("addr", "", "redis addr.eg:127.0.0.1:6379")
	redisType := ss.String("type", "standalone", "redis type:standalone/cluster")
	sortBy := ss.String("sorby", "Time", "sortBy:Instance/Command/Duration/Time")

	switch os.Args[1] {
	case "slowlog":
		ss.Parse(os.Args[2:])
		if *addr == "" {
			ss.Usage()
			os.Exit(0)
		}
		showSlowLog.Run(*addr, *redisType, *sortBy)
	}

}
