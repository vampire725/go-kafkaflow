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

func EventsAck(events []*Event) {
	for _, e := range events {
		e.Ack()
	}
}

type DestFunc func(chan *Event)
type SourceFunc func() chan *Event
type MapFunc func(event *Event) (*Event, bool, error)
type WindowFunc func(events []*Event) ([]*Event, bool, error)
