package main

import (
	"context"
	"flag"
	"net/http"
	"time"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"

	"go2ch/go2ch/ch"
	"go2ch/go2ch/config"
	"go2ch/go2ch/filter"
	"go2ch/go2ch/handler"
	kf "go2ch/go2ch/kafka"
	"go2ch/go2ch/producer/pusher"
)

var configFile = flag.String("f", "etc/config.yml", "Specify the config file")

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
	proc.SetTimeToForceQuit(time.Duration(c.GracePeriodSecond) * time.Second)

	// create a new go-zero service group
	group := service.NewServiceGroup()
	defer group.Stop()

	for _, cluster := range c.Clusters {
		// clickhouse writer
		ctx := context.Background()
		chWriter, err := ch.NewWriter(ctx, cluster.Output.ClickHouse)
		if err != nil {
			panic(err)
		}

		// data filters
		filters := filter.CreateFilters(cluster)

		// data handler
		handle := handler.NewHandler(chWriter)
		handle.AddFilters(filters...)
		//handle.AddFilters(filter.AddUriFieldFilter("url", "uri"))

		// kafka
		ks := config.GetKafkaConf(cluster.Input.Kafka)
		for _, k := range ks {
			mq, err := kq.NewQueue(*k, handle)
			if err != nil {
				panic(err)
			}
			group.Add(mq)
		}

		// start a service to support pull and send data
		kw, err := kf.NewWriter(context.Background(), cluster.Input.Kafka)
		if err != nil {
			panic(err)
		}
		kp := pusher.NewPusher(kw)
		ser, err := rest.NewServer(rest.RestConf{
			Port: cluster.Input.Kafka.Pusher.Port,
			ServiceConf: service.ServiceConf{
				Log: logx.LogConf{
					Path: "./log/go2ch/kafka",
				},
			},
		})
		if err != nil {
			panic(err)
		}
		ser.AddRoutes([]rest.Route{
			{
				Path:    "/pushOne",
				Method:  http.MethodPost,
				Handler: kp.PushOne,
			},
			{
				Path:    "/pushList",
				Method:  http.MethodPost,
				Handler: kp.PushList,
			},
		})
		group.Add(ser)
	}

	// start go-zero service
	group.Start()

}
