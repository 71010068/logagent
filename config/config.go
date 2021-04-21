package config

type AppConfig struct {
	KafkaConfig	`ini:"kafka"`
	EtcdConfig	`ini:"etcd"`
}

type EtcdConfig struct {
	Address string `ini:"address"`
	Key string `ini:"collect_log_key"`
	Timeout int `ini:"timeout"`
}
type KafkaConfig struct {
	Address string	`ini:"address"`
	ChanMaxSize int `ini:"chan_max_size"`

}

// -----unused-----
type TaillogConfig struct {
	FileName string	`ini:"filename"`
}