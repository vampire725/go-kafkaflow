package kafkastreams

import "log"

/*
 * @Author: Gpp
 * @File:   pipeline.go
 * @Date:   2021/9/10 12:12 下午
 */

type Pipeline struct {
	Source        SourceFunc
	Handlers      []Handler
	Destination   DestFunc
	Input         chan *Event
	Output        chan *Event
	Logger        log.Logger
	EnableTimeLog bool
}

type PipelineManger struct {
	Source      SourceFunc
	Pipelines   map[string]*Pipeline
	Destination DestFunc
}

//func NewPipeline()*Pipeline {
//
//}

func (p *PipelineManger) Start() {
	var input chan *Event
	go func() {
		for _, s := range p.Pipelines {
			go func(s *Pipeline) {
				for {
					in := s.Input
					out := s.Output
					for _, h := range s.Handlers {
						// out是每次handler处理后的数据
						out = make(chan *Event)
						h.Init(in, out)
						in = out
					}
					// 存储最终处理完成的out
					p.Destination(out)
				}
			}(s)
		}
	}()

	for {
		// 监听Kafka消费，消费的数据都放到input channel里
		input = p.Source()
		for e := range input {
			// pipeline manager根据不同的topic分发pipeline
			p.Pipelines[e.RawMessage.Topic].Input <- e
		}
	}
}
