package pusher

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	kf "go2ch/go2ch/kafka"
)

type Pusher struct {
	kafka *kf.Writer
}

// NewPusher returns a new kafka pusher
func NewPusher(k *kf.Writer) *Pusher {
	return &Pusher{
		kafka: k,
	}
}

// PushOne pushes one data to kafka
func (p *Pusher) PushOne(resp http.ResponseWriter, req *http.Request) {
	bs, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.Write([]byte(fmt.Sprintf("Send | read body failed:%v", err)))
		return
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(bs, &m)
	if err != nil {
		resp.Write([]byte(fmt.Sprintf("Send | unmarshal body bytes to map failed:%v", err)))
		return
	}
	err = p.kafka.Produce(m)
	if err != nil {
		resp.Write([]byte(fmt.Sprintf("Send | send one data to kafka failed:%v", err)))
		return
	}
	resp.Write([]byte("push data successfully"))
	return
}

// PushList pushes a list of data to kafka
func (p *Pusher) PushList(resp http.ResponseWriter, req *http.Request) {
	bs, err := ioutil.ReadAll(req.Body)
	if err != nil {
		resp.Write([]byte(fmt.Sprintf("Send | read body failed:%v", err)))
		return
	}
	data := make([]interface{}, 0)
	err = json.Unmarshal(bs, &data)
	if err != nil {
		resp.Write([]byte(fmt.Sprintf("Send | unmarshal body bytes to map failed:%v", err)))
		return
	}
	err = p.kafka.Produce(data...)
	if err != nil {
		resp.Write([]byte(fmt.Sprintf("Send | send one data to kafka failed:%v", err)))
		return
	}
	resp.Write([]byte("push data successfully"))
	return
}
