package ch

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go2ch/go2ch/config"
	"go2ch/go2ch/filter"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	jsoniter "github.com/json-iterator/go"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type Writer struct {
	ctx                  context.Context
	conn                 driver.Conn
	executor             *executors.ChunkExecutor
	ddl                  string
	distributedDDL       string
	tableName            string
	distributedTableName string
	columns              []*rowDesc
}

type rowDesc struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	DefaultType       string `json:"default_type"`
	DefaultExpression string `json:"default_expression"`
	Comment           string `json:"comment"`
	CodecExpression   string `json:"codec_expression"`
	TTLExpression     string `json:"ttl_expression"`
}

var index int64 = 0
var total int64 = 0
var size int64 = 0

// NewWriter creates a new writer for clickhouse
func NewWriter(ctx context.Context, c *config.ClickHouseConf) (*Writer, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: c.Addrs,
		Auth: clickhouse.Auth{
			Database: c.Database,
			Username: c.Username,
			Password: c.Password,
		},
		DialTimeout:     time.Duration(c.DialTimeoutSecond) * time.Second,
		MaxOpenConns:    c.MaxOpenConns,
		MaxIdleConns:    c.MaxIdleConns,
		ConnMaxLifetime: time.Duration(c.ConnMaxLiftTimeMinute) * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("newWriter | create clickhouse writer failed: %v", err)
	}

	writer := &Writer{
		ctx:                  ctx,
		conn:                 conn,
		ddl:                  c.DDL,
		distributedDDL:       c.DistributedDDL,
		tableName:            c.TableName,
		distributedTableName: c.DistributedTableName,
	}

	err = writer.initTable()
	if err != nil {
		return nil, fmt.Errorf("newWriter | %v", err)
	}

	writer.executor = executors.NewChunkExecutor(writer.execute, executors.WithChunkBytes(c.MaxChunkBytes), executors.WithFlushInterval(time.Duration(c.FlushIntervalSecond)*time.Second))
	return writer, nil
}

// initTable inits clickhouse table by executing ddl
func (w *Writer) initTable() error {
	err := w.conn.Exec(w.ctx, w.ddl)
	if err != nil {
		return fmt.Errorf("initTable | exec clickhouse table init sql failed: %v", err)
	}

	if strings.TrimSpace(w.distributedDDL) != "" {
		err = w.conn.Exec(w.ctx, w.distributedDDL)
		if err != nil {
			return fmt.Errorf("initTable | exec clickhouse distributed table init sql failed: %v", err)
		}
	}

	rowDescs, err := w.getColumns()
	if err != nil {
		return fmt.Errorf("initTable | get table columns failed: %v", err)
	}
	w.columns = rowDescs

	return nil
}

// Write writes data to chunk executor, when chunk is filled or chunk flash time is met, it would run writer.execute function
func (w *Writer) Write(data string) error {
	err := w.executor.Add(data, len(data))
	if err != nil {
		return fmt.Errorf("write | write data to chunk executor failed: %v", err)
	}
	return nil
}

// execute sends chunk values to clickhouse, it would be called when the chunk is full or reaches flash interval time
func (w *Writer) execute(values []interface{}) {

	index++

	start := time.Now().UnixNano()

	batch, err := w.conn.PrepareBatch(w.ctx, "INSERT INTO "+w.tableName)
	if err != nil {
		logx.Errorf("execute | prepare clickhouse insert batch sql failed: %v", err)
		return
	}
	var length = 0
	for _, value := range values {
		length += len(value.(string))

		m := make(map[string]interface{})
		err = jsoniter.Unmarshal([]byte(value.(string)), &m)
		if err != nil {
			logx.Errorf("execute | unmarshal value failed: %v", err)
			return
		}
		//fmt.Println("execute - m:", m)
		stru, err := w.getDataStruct(m)
		if err != nil {
			logx.Errorf("execute | convert data to struct failed: %v", err)
			return
		}
		//fmt.Println("execute - stru:", stru)
		err = batch.Append(stru...)
		if err != nil {
			logx.Errorf("execute | append value to clickhouse insert batch failed: %v", err)
			return
		}
	}

	err = batch.Send()
	if err != nil {
		logx.Errorf("execute | send values to clickhouse failed: %v", err)
		return
	}

	end := time.Now().UnixNano()
	use := end - start
	total += use
	size += int64(length)

	fmt.Printf("execute function | finish send time: %d\n", end)

	fmt.Printf("execute function | send index=%d, one data size=%dbytes, total data size=%dbytes, one use time=%dns, total use time=%dns, avg=%dns\n", index, length, size, use, total, total/index)
}

// getColumns returns the descriptions of rows in clickhouse table
func (w *Writer) getColumns() ([]*rowDesc, error) {

	rows, err := w.conn.Query(w.ctx, "DESC TABLE "+w.tableName)
	if err != nil {
		return nil, fmt.Errorf("getColumns | query desc table failed: %v", err)
	}

	descs := make([]*rowDesc, 0)

	for rows.Next() {
		desc := rowDesc{}
		err = rows.Scan(&desc.Name,
			&desc.Type,
			&desc.DefaultType,
			&desc.DefaultExpression,
			&desc.Comment,
			&desc.CodecExpression,
			&desc.TTLExpression)
		if err != nil {
			return nil, fmt.Errorf("getColumns | scan row to struct failed: %v", err)
		}
		descs = append(descs, &desc)
	}

	return descs, nil
}

// getDataStruct dynamically builds a struct (in []interface{} format, each interface{} means a filed in struct) according to m
func (w *Writer) getDataStruct(m map[string]interface{}) ([]interface{}, error) {
	stru := make([]interface{}, 0)
	for _, column := range w.columns {
		if v, ok := m[column.Name]; ok {
			switch column.Type {
			// string
			case "String":
				realType, _ := v.(string)
				stru = append(stru, realType)
			// int
			case "Int8":
				if realType, ok := v.(int); ok {
					stru = append(stru, int8(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, int8(rt))
				}
			case "Int16":
				if realType, ok := v.(int); ok {
					stru = append(stru, int16(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, int16(rt))
				}
			case "Int32":
				if realType, ok := v.(int); ok {
					stru = append(stru, int32(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, int32(rt))
				}
			case "Int64":
				if realType, ok := v.(int); ok {
					stru = append(stru, int64(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, int64(rt))
				}
			case "UInt8":
				if realType, ok := v.(int); ok {
					stru = append(stru, uint8(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, uint8(rt))
				}
			case "UInt16":
				if realType, ok := v.(int); ok {
					stru = append(stru, uint16(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, uint16(rt))
				}
			case "UInt32":
				if realType, ok := v.(int); ok {
					stru = append(stru, uint32(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, uint32(rt))
				}
			case "UInt64":
				if realType, ok := v.(int); ok {
					stru = append(stru, uint64(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, uint64(rt))
				}
			// float
			case "Float32":
				if realType, ok := v.(int); ok {
					stru = append(stru, float32(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, float32(rt))
				}
			case "Float64":
				if realType, ok := v.(int); ok {
					stru = append(stru, float64(realType))
				} else {
					rt, _ := v.(float64)
					stru = append(stru, rt)
				}
			default:
				realType, _ := v.(string)
				if strings.Contains(column.Type, "Date") {
					// Date
					// Datetime
					// Datetime64
					vt, err := w.getTimeValue(v, m)
					if err != nil {
						return nil, fmt.Errorf("getDateStruct | %v", err)
					}
					stru = append(stru, vt)
				} else if strings.Contains(column.Type, "FixString") {
					// FixString(N)
					length := column.Type[len("FixString(") : len(column.Type)-1]
					l, _ := strconv.Atoi(length)
					realType = realType[:l]
					stru = append(stru, realType)
				} else if strings.Contains(column.Type, "Enum") {
					// Enum8, Enum16
					stru = append(stru, realType)
				} else if strings.Contains(column.Type, "Array") {
					// Array(T)
					t := column.Type[len("Array(") : len(column.Type)-1]
					vs, _ := v.([]interface{})
					array, err := w.getArray(t, vs, m)
					if err != nil {
						return nil, fmt.Errorf("getDataStruct | get array failed: %v", err)
					}
					stru = append(stru, array)
				}
			}
		}
	}

	return stru, nil
}

// getArray builds a slice data for inserting to clickhouse
func (w *Writer) getArray(t string, vs []interface{}, m map[string]interface{}) ([]interface{}, error) {

	array := make([]interface{}, 0)
	var err error

	switch t {
	// string
	case "String":
		for _, v := range vs {
			realType, _ := v.(string)
			array = append(array, realType)
		}

	case "Int8":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, int8(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, int8(rt))
			}
		}
	case "Int16":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, int16(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, int16(rt))
			}
		}
	case "Int32":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, int32(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, int32(rt))
			}
		}
	case "Int64":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, int64(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, int64(rt))
			}
		}
	case "UInt8":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, uint8(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, uint8(rt))
			}
		}
	case "UInt16":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, uint16(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, uint16(rt))
			}
		}
	case "UInt32":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, uint32(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, uint32(rt))
			}
		}
	case "UInt64":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, uint64(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, uint64(rt))
			}
		}
	// float
	case "Float32":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, float32(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, float32(rt))
			}
		}
	case "Float64":
		for _, v := range vs {
			if realType, ok := v.(int); ok {
				array = append(array, float64(realType))
			} else {
				rt, _ := v.(float64)
				array = append(array, float64(rt))
			}
		}
	default:
		if strings.Contains(t, "Date") {
			// Date
			// Datetime
			// Datetime64
			for _, v := range vs {
				// Date
				// Datetime
				// Datetime64
				var vt time.Time
				if vt, err = w.getTimeValue(v, m); err == nil {
					array = append(array, vt)
				} else {
					break
				}
			}
		} else if strings.Contains(t, "FixString") {
			// FixString(N)
			for _, v := range vs {
				realType, _ := v.(string)
				length := t[len("FixString(") : len(t)-1]
				l, _ := strconv.Atoi(length)
				realType = realType[:l]
				array = append(array, realType)
			}
		} else if strings.Contains(t, "Enum") {
			// Enum8, Enum16
			for _, v := range vs {
				realType, _ := v.(string)
				array = append(array, realType)
			}
		} else if strings.Contains(t, "Array") {
			// 	not support multidimensional arrays
			err = fmt.Errorf("getArray | not support multidimensional arrays")
		}

	}
	return array, err
}

// getTimeValue converts v to time.Time type
func (w *Writer) getTimeValue(v interface{}, m map[string]interface{}) (time.Time, error) {
	vs, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("getTimeValue | time data's original type must be string")
	}
	layout, ok1 := m[filter.Time_Format_Layout_Field]
	local, ok2 := m[filter.Time_Format_Local_Field]
	if !ok1 || !ok2 {
		return time.Time{}, fmt.Errorf("getTimeValue | convert [%v] to time.Time failed, please add time_fortmat config in configuration file if you need to storage time data", v)
	}

	l, err := time.LoadLocation(local.(string))
	if err != nil {
		return time.Time{}, fmt.Errorf("getTimeValue | load location[%v] failed: %v", local, err)
	}

	t, err := time.ParseInLocation(layout.(string), vs, l)
	if err != nil {
		return time.Time{}, fmt.Errorf("getTimeValue | parse date [%v] to time.Time failed: %v", vs, err)
	}

	return t, nil
}
