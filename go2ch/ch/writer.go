package ch

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"go2ch/go2ch/config"
	"time"
)

type Writer struct {
	ctx       context.Context
	conn      driver.Conn
	executor  *executors.ChunkExecutor
	ddl       string
	tableName string
}

// NewWriter creates a new writer for clickhouse
func NewWriter(ctx context.Context, c config.ClickHouseConf) (*Writer, error) {
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
		ctx:       ctx,
		conn:      conn,
		ddl:       c.DDL,
		tableName: c.TableName,
	}

	writer.executor = executors.NewChunkExecutor(writer.execute, executors.WithChunkBytes(c.MaxChunkBytes), executors.WithFlushInterval(time.Duration(c.FlushIntervalSecond)*time.Second))
	return writer, nil
}

// InitTable inits clickhouse table by executing ddl
func (w *Writer) InitTable() error {
	err := w.conn.Exec(w.ctx, w.ddl)
	if err != nil {
		return fmt.Errorf("initTable | exec clickhouse table init sql failed: %v", err)
	}
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
	batch, err := w.conn.PrepareBatch(w.ctx, "INSERT INTO "+w.tableName)
	if err != nil {
		logx.Errorf("execute | prepare clickhouse insert batch sql failed: %v", err)
	}
	err = batch.Append(values...)
	if err != nil {
		logx.Errorf("execute | append values to clickhouse insert batch failed: %v", err)
	}
	err = batch.Send()
	if err != nil {
		logx.Errorf("execute | send values to clickhouse failed: %v", err)
	}
}
