package main

import (
	"context"
	"flag"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/service"
	"go2ch/go2ch/ch"
	"go2ch/go2ch/config"
	"go2ch/go2ch/filter"
	"go2ch/go2ch/handler"
)

var configFile = flag.String("f", "etc/config,.yml", "Specify the config file")

func main() {

	// adds a flag "f" to this program
	// user can use it to specify the config file
	// or use the default config file in etc/config.yaml
	flag.Parse()

	// unmarshal config file
	c, err := config.ReadConfig(*configFile)
	if err != nil {
		panic(err)
	}

	// sets the waiting time before force quitting.
	proc.SetTimeToForceQuit(c.GracePeriod)

	// create a new go-zero service group
	group := service.NewServiceGroup()
	defer group.Stop()

	for _, cluster := range c.Clusters {
		// clickhouse writer
		ctx := context.Background()
		chWriter, err := ch.NewWriter(ctx, cluster.Output.ClickHouse)
		logx.Must(err)

		// data filters
		filters := filter.CreateFilters(cluster)

		// data handler
		handle := handler.NewHandler(chWriter)
		handle.AddFilters(filters...)
		handle.AddFilters(filter.AddUriFieldFilter("url", "uri"))

		// kafka
		ks := config.GetKafkaConf(&cluster.Input.Kafka)
		for _, k := range ks {
			mq, err := kq.NewQueue(*k, handle)
			logx.Must(err)
			group.Add(mq)
		}
	}

	// start go-zero service
	group.Start()
}
