package main

import (
	"context"
	"time"

	"github.com/grpc-boot/bkafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
	"go.uber.org/zap/zapcore"
)

var (
	producerConf bkafka.Config
	producer     *bkafka.Producer
	topic        = `bkafka_test`
	total        = 102400
	startAt      time.Time
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
		"bootstrap.servers":   "127.0.0.1:39092",
		"batch.num.messages":           1024
	}`

	producerConf, err = bkafka.LoadJsonConf4Producer([]byte(confJson))
	if err != nil {
		base.RedFatal("load producer config error:%s", err)
	}

	producer, err = bkafka.NewProducer(producerConf, func(event kafka.Event, done bool) {
		if msg, ok := event.(*kafka.Message); ok {
			if msg.TopicPartition.Error != nil {
				base.Red("delivery message[%s]:%s failed at partition:%d\n",
					base.Bytes2String(msg.Key),
					msg.Value,
					msg.TopicPartition.Partition,
				)
			}
		}
	})

	if err != nil {
		base.RedFatal("init producer error:%s\n", err.Error())
	}
}

func main() {
	defer func() {
		producer.Close()
		shutDown()
	}()

	startAt = time.Now()
	current := startAt.UnixNano()
	for start := 0; start < total; start++ {
		value, _ := base.JsonMarshal(map[string]interface{}{
			"Id": current + int64(start),
		})
		producer.ProduceMsgAsync(topic, value)
		//producer.ProduceOrderAsync(topic, value, 1657780437524912117)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err := producer.FlushAll(ctx)
	if err != nil {
		base.Red("flush msg error:%s\n", err.Error())
	}

	base.Green("successTotal:%d, failedTotal:%d successRate:%.4f\n", producer.TotalSuccess(), producer.TotalFailed(), producer.SuccessRate())
}

func shutDown() {
	base.Green("produce %d cost:%s\n", total, time.Since(startAt))
}
