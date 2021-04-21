package config

type AppConfig struct {
	KafkaConfig	`ini:"kafka"`
	TaillogConfig	`ini:"taillog"`
}

type KafkaConfig struct {
	Address string	`ini:"address"`
	Topic string	`ini:"topic"`
}
type TaillogConfig struct {
	FileName string	`ini:"filename"`
}