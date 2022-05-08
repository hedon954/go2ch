package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/hpcloud/tail"
	"github.com/shopspring/decimal"

	"go2ch/go2ch/config"
	kf "go2ch/go2ch/kafka"
	"go2ch/go2ch/util"
)

/**
  producer example: logger
  description:
		the example shows how to read log info from log file and then send it to kafka.
		after send messages to kafka, we will look how go2ch receives messages then send them to clickhouse.
  test working directory: go2ch/go2ch/producer/logger/
*/

type Log struct {
	Id              uint32          `json:"id"`
	FId             float64         `json:"f_id"`
	DId             decimal.Decimal `json:"d_id"`
	LoggerID        string          `json:"logger_id"`
	Info            string          `json:"info"`
	TransferField   string          `json:"transfer_field"`
	CreateTime      string          `json:"create_time"`
	DropKeyContains string          `json:"drop_key_contains"`
	DropKeyMatch    string          `json:"drop_key_match"`
	RemoveField1    string          `json:"remove_field_1"`
	RemoveField2    string          `json:"remove_field_2"`
}

func main() {

	configPath := "logger.yml"
	logPath := "logger.log"

	c, err := config.ReadConfig(configPath)
	if err != nil {
		panic(err)
	}

	// create kafka producer
	kw, err := kf.NewWriter(context.Background(), c.Clusters[0].Input.Kafka)
	if err != nil {
		panic(err)
	}
	defer kw.Writer.Close()

	// create log file tail tool
	t, err := tail.TailFile(logPath, tail.Config{
		Follow: true,
	})
	if err != nil {
		panic(err)
	}

	// imitate write log
	writeLog(logPath)

	i := 0
	for line := range t.Lines {
		err = kw.Produce(line)
		if err != nil {
			panic(err)
		}
		i++
		fmt.Printf("id: %d, send time: %d\n", i, time.Now().UnixNano())
	}

	// blocking, continue monitoring
	select {}

}

func writeLog(path string) {

	go func() {

		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		for i := 0; i < 100; i++ {
			dropKeyContains := ""
			dropKeyMatch := ""
			if i%2 == 0 {
				dropKeyContains = fmt.Sprintf("drop-contains-%d", i)
				dropKeyMatch = fmt.Sprintf("drop-match")

			} else {
				dropKeyContains = fmt.Sprintf("drop-not-contains-%d", i)
				dropKeyMatch = fmt.Sprintf("drop-not-match-%d", i)
			}
			l := Log{
				Id:              uint32(i),
				FId:             float64(i),
				DId:             decimal.NewFromFloat(222.22),
				LoggerID:        fmt.Sprintf("logger-%d", i),
				Info:            fmt.Sprintf("this is logger info-%d", i),
				TransferField:   fmt.Sprintf("transfer-field-%d", i),
				CreateTime:      time.Now().AddDate(0, 0, i).Format(util.TimestampFormat_Datetime),
				DropKeyContains: dropKeyContains,
				DropKeyMatch:    dropKeyMatch,
				RemoveField1:    fmt.Sprintf("remove-field-1-%d", i),
				RemoveField2:    fmt.Sprintf("remove-field-2-%d", i),
			}

			bs, err := json.Marshal(&l)
			if err != nil {
				panic(err)
			}
			_, err = f.WriteString(string(bs) + "\n")
			//fmt.Println("producer write - " + string(bs))
			if err != nil {
				panic(err)
			}

			//time.Sleep(5 * time.Second)
		}
	}()
}
