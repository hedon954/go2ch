package puller

import kf "go2ch/go2ch/kafka"

type Puller struct {
	kafka *kf.Writer
}

// NewPuller returns a new kafka puller
func NewPuller(k *kf.Writer) *Puller {
	return &Puller{
		kafka: k,
	}
}
