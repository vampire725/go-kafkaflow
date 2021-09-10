package kafkastreams

import (
	"github.com/go-kit/kit/log"
	"time"
)

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

func NewPipeline(logger log.Logger, source SourceFunc, destination DestFunc, enableTimeLogMiddleware bool) *Pipeline {
	return &Pipeline{
		Source:        source,
		Handlers:      make([]Handler, 0),
		Destination:   destination,
		Logger:        logger,
		EnableTimeLog: enableTimeLogMiddleware,
	}
}

func (p *Pipeline) Map(f MapFunc, limit int, name string) *Pipeline {

	p.Handlers = append(p.Handlers, mapHandler{
		limit: limit,
		f:     f,
	})
	return p
}

func (p *Pipeline) Window(f WindowFunc, limit int, duration time.Duration, size int, name string) *Pipeline {
	p.Handlers = append(p.Handlers, &windowHandler{
		limit:    limit,
		duration: duration,
		size:     size,
		f:        f,
	})
	return p
}

func (p *Pipeline) Start() {
	var output, input chan *Event
	for {
		input = p.Source()
		for _, h := range p.Handlers {
			output = make(chan *Event)
			h.Init(input, output)
			input = output
		}
		p.Destination(output)
	}
}
