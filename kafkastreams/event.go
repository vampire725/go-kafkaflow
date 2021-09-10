package kafkastreams

import "github.com/Shopify/sarama"

/*
 * @Author: Gpp
 * @File:   event.go
 * @Date:   2021/9/10 12:12 下午
 */

type Event struct {
	RawMessage *sarama.ConsumerMessage
	Data       interface{}
	Reply      chan struct{}
	SaveEvents []SaveEvent
	Timestamp  int64
}

type SaveEvent struct {
	Event     string
	Value     interface{}
	Timestamp int64
}

func (e *Event) Ack() {
	close(e.Reply)
}

type DestFunc func(chan *Event)
type SourceFunc func() chan *Event
