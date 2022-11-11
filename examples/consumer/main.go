package main

import (
	"github.com/grpc-boot/bkafka"
	"github.com/grpc-boot/bkafka/zapkey"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	consumer     *bkafka.Consumer
	consumerConf bkafka.Config
	key          = `attention`
	topic        = `bkafka_test`
)

func init() {
	err := base.InitZapWithOption(zaplogger.Option{
		Level: int8(zapcore.DebugLevel),
		Path:  "../",
	})
	if err != nil {
		base.RedFatal("init logger error:%s", err)
	}

	confJson := `{
		"bootstrap.servers":        "127.0.0.1:39092",
		"group.id":                 "bkafka_test",
		"auto.offset.reset":        "earliest"
	}`

	consumerConf, err = bkafka.LoadJsonConf4Consumer([]byte(confJson))
	if err != nil {
		base.RedFatal("load consumer config error:%s", err)
	}

	consumer, err = bkafka.NewConsumer(consumerConf)
	if err != nil {
		base.RedFatal("init consumer error:%s", err)
	}
}

func main() {
	err := consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		base.RedFatal("subscribe topics error:%s\n", err.Error())
	}

	defer shutDown()

	consumer.HandlerEvent(func(event kafka.Event, done bool) {
		if e, ok := event.(*kafka.Message); ok {
			base.Green("consumer msg topic:%s partition:%d key:%s value:%s offset:%d total:%d\n", *e.TopicPartition.Topic, e.TopicPartition.Partition, base.Bytes2String(e.Key), e.Value, e.TopicPartition.Offset, consumer.Total())

			base.Info(base.Bytes2String(e.Value))

			base.Info("consume msg",
				zap.String("Key", key),
				zaplogger.Value(e.Value),
				zapkey.Topic(*e.TopicPartition.Topic),
				zap.Int32("Partition", e.TopicPartition.Partition),
				zapkey.Offset(int64(e.TopicPartition.Offset)),
				zap.Int64("Total", consumer.Total()),
			)
		}
	})
}

func shutDown() {
	base.Green("shutdown\n")
}
