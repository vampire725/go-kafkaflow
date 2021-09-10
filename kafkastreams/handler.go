package kafkastreams

/*
 * @Author: Gpp
 * @File:   handler.go
 * @Date:   2021/9/10 12:12 下午
 */

type Handler interface {
	Init(input, output chan *Event)
}
