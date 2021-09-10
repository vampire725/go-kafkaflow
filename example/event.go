package main

const Event0 = "test0"

const Event1 = "test1"

const Event2 = "test2"

func NewEventTopics() map[string]string {
	return map[string]string{
		Event0: "topic0",
		Event1: "topic1",
		Event2: "topic2",
	}
}
