package handler

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"go2ch/go2ch/ch"
	"go2ch/go2ch/filter"
)

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
	for _, filter := range filters {
		mh.filters = append(mh.filters, filter)
	}
}

// Consume writes data to clickhouse execute chunk
func (mh *MessageHandler) Consume(key, value string) error {
	var m map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(value), &m); err != nil {
		return fmt.Errorf("consume | unmarshal value to map failed: %v", err)
	}

	for _, filter := range mh.filters {
		if m = filter(m); m == nil {
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

	return nil
}
