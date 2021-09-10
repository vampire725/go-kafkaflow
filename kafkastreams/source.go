package kafkastreams

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

/*
 * @Author: Gpp
 * @File:   source.go
 * @Date:   2021/9/10 12:12 下午
 */

type TopicHandler struct {
	out     chan *Event
	waitAck chan *Event
	logger  log.Logger
}

func NewTopicHandler(out chan *Event, waitAckSize int, logger log.Logger) sarama.ConsumerGroupHandler {
	return &TopicHandler{
		out:     out,
		waitAck: make(chan *Event, waitAckSize),
		logger:  logger,
	}
}

func (h *TopicHandler) Setup(session sarama.ConsumerGroupSession) error {
	_ = level.Info(h.logger).Log("event", "start")
	go func() {
		for {
			select {
			case event := <-h.waitAck:
				_ = level.Info(h.logger).Log("event", "cache", "wait ack length", len(h.waitAck))
				select {
				case <-event.Reply:
					session.MarkMessage(event.RawMessage, "")
					_ = level.Info(h.logger).Log("event", "mark", "topic", event.RawMessage.Topic, "partition", event.RawMessage.Partition, "offset", event.RawMessage.Offset, "wait ack length", len(h.waitAck))
				}
			case <-session.Context().Done():
				return
			}
		}
	}()
	return nil
}

func (h *TopicHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *TopicHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	go func() {
		for {
			select {
			case message, ok := <-claim.Messages():
				if !ok {
					return
				}
				_ = h.logger.Log("event", "consume", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset)
				event := &Event{
					RawMessage: message,
					Reply:      make(chan struct{}),
				}
				h.out <- event
				h.waitAck <- event
			case <-session.Context().Done():
				return
			}
		}
	}()
	<-session.Context().Done()
	return session.Context().Err()
}

func NewKafkaSource(logger log.Logger, consumerGroup sarama.ConsumerGroup, topics []string, waitAckSize int) SourceFunc {
	return func() chan *Event {
		out := make(chan *Event)
		go func() {
			for {
				handler := NewTopicHandler(out, waitAckSize, logger)
				_ = logger.Log("event", "register", "topic", topics)
				if err := consumerGroup.Consume(context.Background(), topics, handler); err != nil {
					_ = logger.Log("event", fmt.Sprintf("Kafka consumer.Consume failed with: %v", err))
				}

			}
		}()
		return out
	}
}
