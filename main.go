// logAgent 入口
package main

import (
	"fmt"
	"time"

	"gopkg.in/ini.v1"
	"logagent/config"
	"logagent/kafka"
	"logagent/taillog"
)

var (
	cfg = new(config.AppConfig)
)

func run(){
	for {
		// 1 读取日志
		select {
		case line := <- taillog.ReadChan():
			// 2 发送到kafka
			kafka.SendToKafka(cfg.KafkaConfig.Topic,line.Text)
		default:
			time.Sleep(time.Second)
		}
	}
}

func main(){
	// 0 加载配置文件
	//cfg ,err := ini.Load("./config/config.ini")
	err := ini.MapTo(cfg, "./config/config.ini")
	if err != nil{
		fmt.Println("加载配置文件失败，err : ",err)
		return
	}


	// 1 初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConfig.Address})
	if err != nil{
		fmt.Println("初始化kafka失败，err : ",err)
		return
	}
	fmt.Println("初始化kafka成功。")

	// 2 打开日志文件准备收集日志
	err = taillog.Init(cfg.TaillogConfig.FileName)
	if err != nil{
		fmt.Println("初始化taillog失败，err : ",err)
		return
	}
	fmt.Println("初始化taillog成功。")

	run()
}