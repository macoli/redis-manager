package main

import (
	"os"

	"github.com/macoli/redis-manager/pkg/param"

	"github.com/macoli/redis-manager/cmd/check"

	"github.com/macoli/redis-manager/cmd/clsuterconfig"
	"github.com/macoli/redis-manager/cmd/clusterflush"
	"github.com/macoli/redis-manager/cmd/clustermap"
	"github.com/macoli/redis-manager/cmd/moveslot"
	"github.com/macoli/redis-manager/cmd/slowlog"
)

func main() {
	if len(os.Args) < 2 {
		param.Usage()
		os.Exit(0)
	}
	switch os.Args[1] {
	case "slowlog":
		slowlog.Run()
	case "clustermap":
		clustermap.Run()
	case "moveslot":
		moveslot.Run()
	case "clusterflush":
		clusterflush.Run()
	case "clusterconfig":
		clsuterconfig.Run()
	case "check":
		check.Run()
	default:
		param.Usage()
		os.Exit(0)
	}
}
