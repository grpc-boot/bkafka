package bkafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
	"go.uber.org/atomic"
)

type Consumer struct {
	*kafka.Consumer
	conf    *kafka.ConfigMap
	total   atomic.Int64
	groupId string
}

// NewConsumer 实例化Consumer
func NewConsumer(conf Config) (*Consumer, error) {
	cm := conf.ToKafkaConfig()
	rdConsumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		Consumer: rdConsumer,
		conf:     cm,
		groupId:  conf["group.id"].(string),
	}

	return consumer, nil
}

// GroupId 获取groupId
func (c *Consumer) GroupId() string {
	return c.groupId
}

// HandlerEvent 处理事件
func (c *Consumer) HandlerEvent(eh EventHandler) {
	for {
		ev, ok := <-c.Events()
		if !ok {
			eh(ev, true)
			return
		}

		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			base.Error("assigned partitions")
			err := c.Assign(e.Partitions)
			if err != nil {
				base.Error("assigned partitions failed")
			}
		case kafka.RevokedPartitions:
			base.Error("revoked partitions")
			err := c.Unassign()
			if err != nil {
				base.Error("revoked partitions failed")
			}
		case kafka.PartitionEOF:
			base.Info("reached")
		case *kafka.Message:
			c.total.Inc()
		case kafka.Error:
			base.Error("consume kafka error",
				zaplogger.Error(e),
			)
		}
		eh(ev, false)
	}
}

// Total 获取总消费数量
func (c *Consumer) Total() int64 {
	return c.total.Load()
}
