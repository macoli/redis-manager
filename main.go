package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/macoli/redis-manager/cmd/showSlowLog"
)

// redis manager project

func usage() {
	fmt.Fprintf(os.Stderr, `Usage of redis-manager:
Options:
  slowlog redis慢查询信息展示
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	// get args from cmd
	ss := flag.NewFlagSet("slowlog", flag.ExitOnError)
	addr := ss.String("addr", "127.0.0.1:6379", "redis addr")
	redisType := ss.String("type", "standalone", "redis type:standalone/cluster")
	sortBy := ss.String("sortby", "Time", "sortBy:Instance/Command/Duration/Time")

	switch os.Args[1] {
	case "slowlog":
		params := os.Args[2:]
		if len(params) == 0 {
			ss.Usage()
			os.Exit(1)
		}
		if err := ss.Parse(os.Args[2:]); err != nil {
			fmt.Printf("get params failed, err:%v\n", err)
			ss.Usage()
			os.Exit(1)
		}
		showSlowLog.Run(*addr, *redisType, *sortBy)
	default:
		usage()
		os.Exit(1)
	}

}
