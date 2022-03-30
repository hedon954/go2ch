package config

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

type Condition struct {
	Key   string `yaml:"Key" json:"key"`
	Value string `yaml:"Value" json:"value"`
	Type  string `yaml:"Type" json:"type,default=match,options=match|contains"`
	Op    string `yaml:"Op" json:"op,default=and,options=and|or"`
}

type ClickHouseConf struct {
	Addrs                 []string `yaml:"Addrs" json:"Addrs"`
	Username              string   `yaml:"Username" json:"username,default=default"`
	Password              string   `yaml:"Password" json:"password,optional"`
	Database              string   `yaml:"Database" json:"database,default=default"`
	TableName             string   `yaml:"TableName" json:"tableName"`
	DDL                   string   `yaml:"DDL" json:"ddl"`
	DialTimeoutSecond     int      `yaml:"DialTimeoutSecond" json:"dialTimeoutSecond,optional,default=5"`
	MaxIdleConns          int      `yaml:"MaxIdleConns" json:"maxIdleConns,optional,default=5"`
	MaxOpenConns          int      `yaml:"MaxOpenConns" json:"maxOpenConns,optional,default=10"`
	ConnMaxLiftTimeMinute int      `yaml:"ConnMaxLiftTimeMinute" json:"connMaxLiftTimeMinute,optional,default=60"`
	MaxChunkBytes         int      `yaml:"MaxChunkBytes" json:"maxChunkBytes"`
	FlushIntervalSecond   int      `yaml:"FlushIntervalSecond" json:"flushIntervalSecond,default=5"`
}

type Filter struct {
	Action     string      `yaml:"Action" json:"action,optional=drop|remove_field|transfer"`
	Conditions []Condition `yaml:"Conditions" json:"conditions,optional"`
	Fields     []string    `yaml:"Fields" json:"fields,optional"`
	Field      string      `yaml:"Field" json:"field,optional"`
	Target     string      `yaml:"Target" json:"target,optional"`
}

type KafkaConf struct {
	service.ServiceConf
	Brokers    []string `yaml:"Brokers" json:"brokers"`
	Group      string   `yaml:"Group" json:"group"`
	Topics     []string `yaml:"Topics" json:"topics"`
	Offset     string   `yaml:"Offset" json:"offset,options=first|last,default=last"`
	Conns      int      `yaml:"Conns" json:"conns,default=1"`
	Consumers  int      `yaml:"Consumers" json:"consumers,default=8"`
	Processors int      `yaml:"Processors" json:"processors,default=8"`
	MinBytes   int      `yaml:"MinBytes" json:"minBytes,default=10240"`    // 10K
	MaxBytes   int      `yaml:"MaxBytes" json:"maxBytes,default=10485760"` // 10M
}

type Cluster struct {
	Input struct {
		Kafka KafkaConf `yaml:"Kafka" json:"kafka"`
	} `yaml:"Input" json:"input"`
	Filters []Filter `yaml:"Filters" json:"filters,optional"`
	Output  struct {
		ClickHouse ClickHouseConf `yaml:"ClickHouse" json:"click_house"`
	} `yaml:"Output"`
}

type Config struct {
	Clusters    []Cluster     `yaml:"Clusters" json:"clusters"`
	GracePeriod time.Duration `json:",default=10s"`
}

// ReadConfig read config file and return a *Config
func ReadConfig(path string) (*Config, error) {
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("readConfig | read file[%s] failed: %v", path, err)
	}
	var c Config
	err = conf.LoadConfigFromYamlBytes(bs, &c)
	if err != nil {
		return nil, fmt.Errorf("readConfig | load config from yaml bytes failed: %v", err)
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
