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
	Action     string      `json:",options=drop|remove_field|transfer|time_format"`
	Conditions []Condition `json:",optional"`
	Fields     []string    `json:",optional"`
	Field      string      `json:",optional"`
	Target     string      `json:",optional"`
	Layout     string      `json:",optional"`
	Local      string      `json:",optional,default=Local"`
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
	Pusher     *KafkaPusher
	Puller     *KafkaPuller
}

type KafkaPusher struct {
	Port int `json:",optional,default=10010"`
}

type KafkaPuller struct {
	Uri    string `json:",optional"`
	Method string `json:",optional"`
	Header string `json:",optional"`
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

// GetKafkaConf returns a list of kafka config
func GetKafkaConf(c *KafkaConf) []*kq.KqConf {
	ret := make([]*kq.KqConf, 0)
	for _, topic := range c.Topics {
		ret = append(ret, &kq.KqConf{
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
