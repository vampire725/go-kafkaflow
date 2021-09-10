package main

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"go-kafkastreams/configutil"
	"go-kafkastreams/kafkastreams"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var cfg struct {
	Kafka configutil.KafkaConfig `yaml:"kafka"`
}

func main() {
	{
		b, err := ioutil.ReadFile("config.yaml")
		if err != nil {
			panic(err)
		}
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			panic(err)
		}
	}
	var logger log.Logger
	{
		logger = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	}

	producer, err := configutil.NewKafkaSyncProducer(cfg.Kafka)
	if err != nil {
		panic(err)
	}
	destination := kafkastreams.NewKafkaDestination(logger, producer, map[string]string{
		Event0: "topic0",
		Event1: "topic1",
		Event2: "topic2",
	})

	{
		textSource, err := kafkastreams.NewKafkaSource(logger, cfg.Kafka, []string{"test0"}, 5)
		if err != nil {
			os.Exit(1)
		}

		p := kafkastreams.NewPipeline(logger, textSource, destination, true)
		p.Map(toUpper, 5, "test0").Window(toWindow, 5, time.Second*5, 5, "window")
		go p.Start()
	}

	{
		source, err := kafkastreams.NewKafkaSource(logger, cfg.Kafka, []string{"test1"}, 5)
		if err != nil {
			os.Exit(1)
		}
		p := kafkastreams.NewPipeline(logger, source, destination, true)
		p.Map(toUpper, 5, "test1").Window(toWindow, 5, time.Second*5, 5, "test1window")
		go p.Start()
	}
	wait()
}

func wait() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		return
	}
}

var toUpper = func(event *kafkastreams.Event) (*kafkastreams.Event, bool, error) {
	event.Data = []byte(strings.ToUpper(string(event.RawMessage.Value)))
	event.SaveEvents = append(event.SaveEvents, kafkastreams.SaveEvent{
		Event: Event0,
		Value: event.Data,
	})
	return event, true, nil
}

var toWindow = func(events []*kafkastreams.Event) ([]*kafkastreams.Event, bool, error) {
	for _, e := range events {
		var str string
		d := e.Data.([]byte)
		str = string(d)
		e.Data = fmt.Sprintf("测试成功:%v", str)
		fmt.Println(e.Data)
		e.SaveEvents = append(e.SaveEvents, kafkastreams.SaveEvent{
			Event: Event1,
			Value: d,
		})
	}
	return events, true, nil
}
