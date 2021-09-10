package kafkastreams

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

/*
 * @Author: Gpp
 * @File:   destination.go
 * @Date:   2021/9/10 11:45 上午
 */

func NewKafkaDestination(logger log.Logger, producer sarama.SyncProducer, eventTopics map[string]string) DestFunc {
	return func(input chan *Event) {
		for e := range input {
			for _, d := range e.SaveEvents {
				b, err := json.Marshal(d.Value)
				if err != nil {
					_ = level.Error(logger).Log("err", "err in send message to kafka destination")
				}
				topic, ex := eventTopics[d.Event]
				if !ex {
					_ = level.Error(logger).Log("err", "err in send message to kafka destination,no event", "event", d.Event)
					continue
				}
				err = send(context.Background(), logger, producer, topic, b)
				if err != nil {
					_ = level.Error(logger).Log("err", "err in send message to kafka destination")
				}
			}
			e.Ack()
		}
	}
}

func send(_ context.Context, logger log.Logger, producer sarama.SyncProducer, topic string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	p, o, err := producer.SendMessage(msg)
	_ = logger.Log("event", "save event", "topic", topic, "value", string(value), "partition", p, "offset", o)
	return err
}
