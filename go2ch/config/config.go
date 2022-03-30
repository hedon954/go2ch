package config

import (
	"fmt"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

type Condition struct {
	Key   string
	Value string
	Type  string `json:",default=match,options=match|contains"`
	Op    string `json:",default=and,options=and|or"`
}

type ClickHouseConf struct {
	Addrs                 []string
	Username              string `json:",default=default"`
	Password              string `json:",optional"`
	Database              string `json:",default=default"`
	TableName             string
	DDL                   string
	DialTimeoutSecond     int `json:",optional,default=5"`
	MaxIdleConns          int `json:",optional,default=5"`
	MaxOpenConns          int `json:",optional,default=10"`
	ConnMaxLiftTimeMinute int `json:",optional,default=60"`
	MaxChunkBytes         int `json:",optional,default=10485760"`
	FlushIntervalSecond   int `json:",default=5"`
}

type Filter struct {
	Action     string      `json:",options=drop|remove_field|transfer"`
	Conditions []Condition `json:",optional"`
	Fields     []string    `json:",optional"`
	Field      string      `json:",optional"`
	Target     string      `json:",optional"`
}

type KafkaConf struct {
	service.ServiceConf
	Name       string
	Brokers    []string
	Group      string
	Topics     []string
	Offset     string `json:",options=first|last,default=last"`
	Conns      int    `json:",default=1"`
	Consumers  int    `json:",default=8"`
	Processors int    `json:",default=8"`
	MinBytes   int    `json:",default=10240"`    // 10K
	MaxBytes   int    `json:",default=10485760"` // 10M
}

type Cluster struct {
	Input   *Input
	Filters []Filter `json:",optional"`
	Output  *Output
}

type Input struct {
	Kafka *KafkaConf
}

type Output struct {
	ClickHouse *ClickHouseConf
}

type Config struct {
	Clusters          []*Cluster
	GracePeriodSecond int `json:",optional,default=10"`
}

// ReadConfig read config file and return a *Config
func ReadConfig(path string) (*Config, error) {
	var c Config
	err := conf.LoadConfig(path, &c)
	if err != nil {
		return nil, fmt.Errorf("readConfig | load config[%v] failed: %v", path, err)
	}
	return &c, nil
}

// buildConfig checks config value's validation and sets config defaults values
func buildConfig(c *Config) error {
	if c.GracePeriodSecond == 0 {
		c.GracePeriodSecond = 10
	}
	if c.Clusters != nil {
		for _, cluster := range c.Clusters {

			// input
			if cluster.Input == nil {
				return fmt.Errorf("buildConfig | lack [Input] configuration in some clusters")
			}

			// input:kafka
			if cluster.Input.Kafka != nil {
				k := cluster.Input.Kafka
				if k.Name == "" {
					return fmt.Errorf("buildConfig | lack [Input:Kafka:Name] in configuration")
				}
				if k.Offset == "" {
					k.Offset = "last"
				}
				if k.Conns == 0 {
					k.Conns = 1
				}
				if k.Consumers == 0 {
					k.Consumers = 8
				}
				if k.Processors == 0 {
					k.Processors = 8
				}
				if k.MinBytes == 0 {
					k.MinBytes = 10240
				}
				if k.MaxBytes == 0 {
					k.MaxBytes = 10485760
				}
			}

			// filters
			for _, filter := range cluster.Filters {
				if filter.Action == "" {
					return fmt.Errorf("buildConfig | lack [Filters:Action] configuration in some clusters")
				}
				if filter.Action != "drop" && filter.Action != "remove_field" && filter.Action != "transfer" {
					return fmt.Errorf("buildConfig | [Filters:Action] only drop, remove_field and transfer are supported")
				}
			}

			// output
			if cluster.Output == nil {
				return fmt.Errorf("buildConfig | lack [Output] configuration in some clusters")
			}

			// output:clickhouse
			if cluster.Output.ClickHouse != nil {
				clickH := cluster.Output.ClickHouse
				if clickH.Username == "" {
					clickH.Username = "default"
				}
				if clickH.Database == "" {
					clickH.Database = "default"
				}
				if clickH.TableName == "" {
					return fmt.Errorf("buildConfig | lack [Output:ClickHouse:TableName] in configuration")
				}
				if clickH.DialTimeoutSecond == 0 {
					clickH.DialTimeoutSecond = 5
				}
				if clickH.MaxIdleConns == 0 {
					clickH.MaxIdleConns = 5
				}
				if clickH.MaxOpenConns == 0 {
					clickH.MaxOpenConns = 5
				}
				if clickH.ConnMaxLiftTimeMinute == 0 {
					clickH.ConnMaxLiftTimeMinute = 60
				}
				if clickH.MaxChunkBytes == 0 {
					clickH.MaxChunkBytes = 10485760
				}
				if clickH.FlushIntervalSecond == 0 {
					clickH.FlushIntervalSecond = 5
				}
			}
		}
	}
	return nil
}

// GetKafkaConf returns a list of kafka config
func GetKafkaConf(c *KafkaConf) []*kq.KqConf {
	ret := make([]*kq.KqConf, 0)
	for _, topic := range c.Topics {
		ret = append(ret, &kq.KqConf{
			// TODO: make it customizable
			//ServiceConf: service.ServiceConf{
			//	Name: c.Name,
			//	Log: logx.LogConf{
			//		ServiceName:         c.Name,
			//		Mode:                "file",
			//		Encoding:            "plain",
			//		TimeFormat:          util.TimestampFormat_Short,
			//		Path:                fmt.Sprintf("log/%s", c.Name),
			//		Level:               "info",
			//		Compress:            false,
			//		KeepDays:            7,
			//		StackCooldownMillis: 100,
			//	},
			//},
			ServiceConf: c.ServiceConf,
			Brokers:     c.Brokers,
			Group:       c.Group,
			Topic:       topic,
			Offset:      c.Offset,
			Conns:       c.Conns,
			Consumers:   c.Consumers,
			Processors:  c.Processors,
			MinBytes:    c.MinBytes,
			MaxBytes:    c.MaxBytes,
		})
	}
	return ret
}
