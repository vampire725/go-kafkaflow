package kafkastreams

import (
	"time"
)

/*
 * @Author: Gpp
 * @File:   handler.go
 * @Date:   2021/9/10 12:12 下午
 */

type Handler interface {
	Init(input, output chan *Event)
}

type mapHandler struct {
	limit int
	f     MapFunc
}

func (h mapHandler) Init(input, output chan *Event) {
	for i := 0; i < h.limit; i++ {
		go func() {
			for e := range input {
				if e, ok, err := h.f(e); ok && err == nil {
					output <- e
				} else if e != nil {
					e.Ack()
				}
			}
			close(output)
		}()
	}
}

type windowHandler struct {
	limit    int
	duration time.Duration
	size     int
	f        WindowFunc
}

func (h windowHandler) Init(input, output chan *Event) {
	go func() {
		batchChan := make(chan []*Event)
		for i := 0; i < h.limit; i++ {
			go func() {
				for batch := range batchChan {
					events, ok, err := h.f(batch)
					if ok && err == nil {
						for _, e := range events {
							output <- e
						}
					} else {
						EventsAck(events)
					}
				}
			}()
		}
		buffer := make([]*Event, 0, h.size)
		timer := time.NewTimer(h.duration)
		for {
		inner:
			for {
				select {
				case e := <-input:
					buffer = append(buffer, e)
					if len(buffer) == h.size {
						break inner
					}
				case <-timer.C:
					break inner
				}
			}
			if len(buffer) > 0 {
				batchChan <- buffer
				buffer = make([]*Event, 0, h.size)
			}
			timer.Stop()
			timer.Reset(h.duration)
		}
	}()
}
