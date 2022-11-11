package bkafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

// EventHandler 事件处理
type EventHandler func(event kafka.Event, done bool)

// DefaultProduceEventHandler 默认生产者事件处理
func DefaultProduceEventHandler(event kafka.Event, done bool) {}
