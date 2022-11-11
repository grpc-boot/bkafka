package zapkey

import "go.uber.org/zap"

func Topic(topic string) zap.Field {
	return zap.String("Topic", topic)
}

func Offset(offset int64) zap.Field {
	return zap.Int64("Offset", offset)
}

func SuccessTotal(total int64) zap.Field {
	return zap.Int64("SuccessTotal", total)
}

func FailTotal(total int64) zap.Field {
	return zap.Int64("FailTotal", total)
}

func SuccessRate(rate float64) zap.Field {
	return zap.Float64("SuccessRate", rate)
}
