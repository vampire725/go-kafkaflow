# go-kafkastreams

基于Kafka编写的流数据处理框架，用一种简单简洁的方式构建数据处理的管道。

## 用法

`example`下有简单事例用法。详情请参考[事例](https://github.com/vampire725/go-kafkaflow/blob/main/example/main.go)

```go

// 简单演示

{
	// Kafka数据存储目的地
	destination := kafkastreams.NewKafkaDestination(logger, producer, map[string]string{
	"Event0": "topic0",
	"Event1": "topic1",
	"Event2": "topic2",
	})

	// Kafka数据来源设置
    source,_ := kafkastreams.NewKafkaSource(logger, cfg.Kafka, []string{"test1"}, 5)
    
	// 初始化pipeline
	p := kafkastreams.NewPipeline(logger, source, destination, true)
    
	// 实现map和滑动窗口的方法
	p.Map(toUpper, 5, "test1").Window(toWindow, 5, time.Second*5, 5, "test1window")
    
	// 启动管道
	go p.Start()
}
```