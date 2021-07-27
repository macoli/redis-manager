package paramDeal

import (
	"flag"
	"fmt"
	"os"
)

func ParamsCheck(p *flag.FlagSet) {
	params := os.Args[2:]
	if len(params) == 0 {
		p.Usage()
		os.Exit(0)
	}
	if err := p.Parse(os.Args[2:]); err != nil {
		fmt.Printf("参数解析失败, err:%v\n", err)
		p.Usage()
		os.Exit(0)
	}
}
