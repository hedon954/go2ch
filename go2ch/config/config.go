package config

import (
	"fmt"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"go2ch/go2ch/util"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Condition struct {
	Key   string `yaml:"Key" json:"Key""`
	Value string `yaml:"Value" json:"Value"`
	Type  string `yaml:"Type" json:"Type,default=match,options=match|contains"`
	Op    string `yaml:"Op" json:"Op,default=and,options=and|or"`
}

type ClickHouseConf struct {
	Addrs                 []string `yaml:"Addrs" json:"Addrs"`
	Username              string   `yaml:"Username" json:"Username,default=default"`
	Password              string   `yaml:"Password" json:"Password,optional"`
	Database              string   `yaml:"Database" json:"Database,default=default"`
	TableName             string   `yaml:"TableName" json:"TableName"`
	DDL                   string   `yaml:"DDL" json:"DDL"`
	DialTimeoutSecond     int      `yaml:"DialTimeoutSecond" json:"DialTimeoutSecond,optional,default=5"`
	MaxIdleConns          int      `yaml:"MaxIdleConns" json:"MaxIdleConns,optional,default=5"`
	MaxOpenConns          int      `yaml:"MaxOpenConns" json:"MaxOpenConns,optional,default=10"`
	ConnMaxLiftTimeMinute int      `yaml:"ConnMaxLiftTimeMinute" json:"ConnMaxLiftTimeMinute,optional,default=60"`
	MaxChunkBytes         int      `yaml:"MaxChunkBytes" json:"MaxChunkBytes,optional,default=10485760"`
	FlushIntervalSecond   int      `yaml:"FlushIntervalSecond" json:"FlushIntervalSecond,default=5"`
}

type Filter struct {
	Action     string      `yaml:"Action" json:"Action,options=drop|remove_field|transfer"`
	Conditions []Condition `yaml:"Conditions" json:"Conditions,optional"`
	Fields     []string    `yaml:"Fields" json:"Fields,optional"`
	Field      string      `yaml:"Field" json:"Field,optional"`
	Target     string      `yaml:"Target" json:"Target,optional"`
}

type KafkaConf struct {
	Name       string   `yaml:"Name" json:"Name"`
	Brokers    []string `yaml:"Brokers" json:"Brokers"`
	Group      string   `yaml:"Group" json:"Group"`
	Topics     []string `yaml:"Topics" json:"Topics"`
	Offset     string   `yaml:"Offset" json:"Offset,options=first|last,default=last"`
	Conns      int      `yaml:"Conns" json:"Conns,default=1"`
	Consumers  int      `yaml:"Consumers" json:"Consumers,default=8"`
	Processors int      `yaml:"Processors" json:"Processors,default=8"`
	MinBytes   int      `yaml:"MinBytes" json:"MinBytes,default=10240"`    // 10K
	MaxBytes   int      `yaml:"MaxBytes" json:"MaxBytes,default=10485760"` // 10M
}

type Cluster struct {
	Input   *Input   `yaml:"Input" json:"Input"`
	Filters []Filter `yaml:"Filters" json:"Filters,optional"`
	Output  *Output  `yaml:"Output" json:"Output"`
}

type Input struct {
	Kafka *KafkaConf `yaml:"Kafka" json:"Kafka"`
}

type Output struct {
	ClickHouse *ClickHouseConf `yaml:"ClickHouse" json:"ClickHouse"`
}

type Config struct {
	Clusters          []*Cluster `yaml:"Clusters" json:"clusters"`
	GracePeriodSecond int        `yaml:"GracePeriod" json:"gracePeriod,optional,default=10"`
}

// ReadConfig read config file and return a *Config
func ReadConfig(path string) (*Config, error) {

	var c Config
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("readConfig | read file[%s] failed: %v", path, err)
	}
	err = yaml.Unmarshal(bs, &c)
	if err != nil {
		return nil, fmt.Errorf("readConfig | load config from yaml bytes failed: %v", err)
	}
	err = buildConfig(&c)
	if err != nil {
		return nil, fmt.Errorf("readConfig | build config failed: %v", err)
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
			ServiceConf: service.ServiceConf{
				Name: c.Name,
				Log: logx.LogConf{
					ServiceName:         c.Name,
					Mode:                "file",
					Encoding:            "plain",
					TimeFormat:          util.TimestampFormat_Short,
					Path:                fmt.Sprintf("log/%s", c.Name),
					Level:               "info",
					Compress:            false,
					KeepDays:            7,
					StackCooldownMillis: 100,
				},
			},
			Brokers:    c.Brokers,
			Group:      c.Group,
			Topic:      topic,
			Offset:     c.Offset,
			Conns:      c.Conns,
			Consumers:  c.Consumers,
			Processors: c.Processors,
			MinBytes:   c.MinBytes,
			MaxBytes:   c.MaxBytes,
		})
	}
	return ret
}
