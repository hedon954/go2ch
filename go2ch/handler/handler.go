package handler

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"go2ch/go2ch/ch"
	"go2ch/go2ch/filter"
	"time"
)

var index int64 = 0
var total int64 = 0

type MessageHandler struct {
	writer  *ch.Writer
	filters []filter.FilterFunc
}

// NewHandler creates a new message handler which is used to consume the message from kafka
func NewHandler(writer *ch.Writer) *MessageHandler {
	return &MessageHandler{
		writer:  writer,
		filters: []filter.FilterFunc{filter.RecoverFilter("kafka")},
	}
}

// AddFilters adds filters
func (mh *MessageHandler) AddFilters(filters ...filter.FilterFunc) {
	for _, f := range filters {
		mh.filters = append(mh.filters, f)
	}
}

// Consume writes data to clickhouse execute chunk
func (mh *MessageHandler) Consume(key, value string) error {

	index++

	start := time.Now().UnixNano()

	var m map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(value), &m); err != nil {
		return fmt.Errorf("consume | unmarshal value to map failed: %v", err)
	}

	for _, f := range mh.filters {
		if m = f(m); m == nil {
			return fmt.Errorf("consume | m became nil")
		}
	}

	bs, err := jsoniter.Marshal(m)
	if err != nil {
		return fmt.Errorf("consume | marshal map to bytes failed: %v", err)
	}

	err = mh.writer.Write(string(bs))
	if err != nil {
		return fmt.Errorf("consume | write data to clickhouse executor chunk failed: %v", err)
	}

	end := time.Now().UnixNano()

	use := end - start
	total += use

	fmt.Printf("consume function | write index=%d, length=%dbytes, one use time: %dns, total use time=%dns, avg=%dns\n", index, len(string(bs)), use, total, total/index)

	return nil
}
