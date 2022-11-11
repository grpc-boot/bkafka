package bkafka

import (
	"context"
	"strconv"

	"github.com/grpc-boot/bkafka/zapkey"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Producer struct {
	*kafka.Producer
	conf         *kafka.ConfigMap
	successTotal atomic.Int64
	failedTotal  atomic.Int64
}

// NewProducer 实例化Producer
func NewProducer(conf Config, eh EventHandler) (*Producer, error) {
	cm := conf.ToKafkaConfig()
	rdProducer, err := kafka.NewProducer(cm)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		Producer: rdProducer,
		conf:     cm,
	}

	if eh == nil {
		eh = DefaultProduceEventHandler
	}

	go producer.handlerEvent(eh)

	return producer, nil
}

// handlerEvent 处理事件
func (p *Producer) handlerEvent(handler EventHandler) {
	for {
		e, ok := <-p.Events()
		if !ok {
			handler(e, true)
			return
		}

		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				failTotal := p.failedTotal.Inc()
				base.Error("delivery msg failed",
					zaplogger.Value(m.Value),
					zapkey.Topic(*m.TopicPartition.Topic),
					zaplogger.Error(m.TopicPartition.Error),
					zapkey.FailTotal(failTotal),
					zapkey.SuccessTotal(p.TotalSuccess()),
					zapkey.SuccessRate(p.SuccessRate()),
				)
			} else {
				successTotal := p.successTotal.Inc()
				if base.IsLevel(zapcore.DebugLevel) {
					base.Debug("delivered message success",
						zapkey.Topic(*m.TopicPartition.Topic),
						zapkey.Offset(int64(m.TopicPartition.Offset)),
						zapkey.FailTotal(p.TotalFailed()),
						zapkey.SuccessTotal(successTotal),
						zapkey.SuccessRate(p.SuccessRate()),
					)
				}
			}
		case kafka.Error:
			base.Error("event error",
				zaplogger.Error(ev),
			)
		default:
			if base.IsLevel(zapcore.DebugLevel) {
				base.Debug("ignored event",
					zaplogger.Event(ev.String()),
				)
			}
		}
		handler(e, false)
	}
}

// id2Key _
func (p *Producer) id2Key(id int64) []byte {
	return []byte(strconv.FormatInt(id, 10))
}

// Produce2Buffer 向librdkafka写消息，错误类型包括：队列已满、超时等
func (p *Producer) Produce2Buffer(topic string, value []byte, partition int32, key []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
		},
		Value: value,
		Key:   key,
	}

	if base.IsLevel(zapcore.DebugLevel) {
		base.Debug("send msg to librdkafka buffer",
			zapkey.Topic(topic),
			zaplogger.Value(value),
			zap.ByteString("Key", key),
		)
	}

	return p.Produce(msg, nil)
}

// ProduceMsg2Buffer 向librdkafka发送消息，错误类型包括：队列已满、超时等
func (p *Producer) ProduceMsg2Buffer(topic string, value []byte) error {
	return p.Produce2Buffer(topic, value, kafka.PartitionAny, nil)
}

// ProduceOrder2Buffer 向librdkafka顺序(同一个id会发送到同一个partition)发送消息
func (p *Producer) ProduceOrder2Buffer(topic string, value []byte, id int64) error {
	return p.Produce2Buffer(topic, value, kafka.PartitionAny, p.id2Key(id))
}

// ProduceMsgAsync 异步向某个topic发送消息
func (p *Producer) ProduceMsgAsync(topic string, value []byte) {
	p.ProduceAsync(topic, value, kafka.PartitionAny, nil)
}

// ProduceOrderAsync 异步向某个topic顺序(同一个id会发送到同一个partition)发送消息
func (p *Producer) ProduceOrderAsync(topic string, value []byte, id int64) {
	p.ProduceAsync(topic, value, kafka.PartitionAny, p.id2Key(id))
}

// ProduceAsync 异步向某个topic发送消息
func (p *Producer) ProduceAsync(topic string, value []byte, partition int32, key []byte) {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
		},
		Value: value,
		Key:   key,
	}

	if base.IsLevel(zapcore.DebugLevel) {
		base.Debug("send msg to librdkafka async",
			zapkey.Topic(topic),
			zaplogger.Value(value),
			zap.ByteString("Key", key),
		)
	}

	p.ProduceChannel() <- msg
}

func (p *Producer) FlushAll(ctx context.Context) (err error) {
	select {
	default:
	case <-ctx.Done():
		return ctx.Err()
	}

	rest := 1
	for rest > 0 {
		rest = p.Flush(500)
	}

	return nil
}

// TotalSuccess 投递成功总量
func (p *Producer) TotalSuccess() int64 {
	return p.successTotal.Load()
}

// TotalFailed 投递失败总量
func (p *Producer) TotalFailed() int64 {
	return p.failedTotal.Load()
}

// SuccessRate 投递成功率
func (p *Producer) SuccessRate() float64 {
	total := p.TotalSuccess() + p.TotalFailed()
	if total == 0 {
		return 1
	}

	return float64(p.TotalSuccess()) / float64(total)
}
