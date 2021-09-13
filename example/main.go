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
	// 生产者设置
	producer, err := configutil.NewKafkaSyncProducer(cfg.Kafka)
	if err != nil {
		panic(err)
	}
	// 为所有管道设置存储Kafka的目的地，其中每一个事件对应一个Topic
	destination := kafkastreams.NewKafkaDestination(logger, producer, map[string]string{
		Event0: "topic0",
		Event1: "topic1",
		Event2: "topic2",
	})

	{
		// 每一个管道都有自己对应的数据来源（Kafka），使用消费者组模式获取数据
		textSource0, err := kafkastreams.NewKafkaSource(logger, cfg.Kafka, []string{"test0"}, 5)
		if err != nil {
			os.Exit(1)
		}
		// 初始化管道
		p := kafkastreams.NewPipeline(logger, textSource0, destination, true)
		// 实现map
		p.Map(toUpper, 5, "test0")
		// 管道启动
		go p.Start()
	}

	{
		textSource1, err := kafkastreams.NewKafkaSource(logger, cfg.Kafka, []string{"test1"}, 5)
		if err != nil {
			os.Exit(1)
		}
		p := kafkastreams.NewPipeline(logger, textSource1, destination, true)
		// 实现window
		p.Window(toWindow, 5, time.Second*5, 5, "test1window")
		go p.Start()
	}

	{
		textSource2, err := kafkastreams.NewKafkaSource(logger, cfg.Kafka, []string{"test2"}, 5)
		if err != nil {
			os.Exit(1)
		}
		p := kafkastreams.NewPipeline(logger, textSource2, destination, true)
		//实现map和window
		p.Map(toUpper, 5, "test2").Window(toWindow, 5, time.Second*5, 5, "test2window")
		go p.Start()
	}
	wait()
}

var toUpper = func(event *kafkastreams.Event) (*kafkastreams.Event, bool, error) {
	// 每一次处理后的数据放在event.Data里，最后需要保存到Kafka的数据放在event.SaveEvents
	event.Data = map[string]string{string(event.RawMessage.Value): strings.ToUpper(string(event.RawMessage.Value))}
	event.SaveEvents = append(event.SaveEvents, kafkastreams.SaveEvent{
		// 添加事件，事件对应topic
		Event: Event0,
		Value: event.Data,
	})
	return event, true, nil
}

var toWindow = func(events []*kafkastreams.Event) ([]*kafkastreams.Event, bool, error) {
	for _, e := range events {
		e.Data = map[string]string{string(e.RawMessage.Value): fmt.Sprintf("测试成功:%v", string(e.RawMessage.Value))}
		e.SaveEvents = append(e.SaveEvents, kafkastreams.SaveEvent{
			Event: Event1,
			Value: e.Data,
		})
	}
	return events, true, nil
}

func wait() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		return
	}
}
