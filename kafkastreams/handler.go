package kafkastreams

import "time"

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

func (m mapHandler) Init(input, output chan *Event) {
	panic("implement me")
}

type windowHandler struct {
	limit    int
	duration time.Duration
	size     int
	f        WindowFunc
}

func (w windowHandler) Init(input, output chan *Event) {
	panic("implement me")
}
