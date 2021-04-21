// logAgent 入口
package main

import (
	"fmt"
	"logagent/etcd"
	"logagent/taillog"
	"logagent/utils"
	"sync"
	"time"

	"gopkg.in/ini.v1"
	"logagent/config"
	"logagent/kafka"
)

var (
	cfg = new(config.AppConfig)
)

//func run(){
//	for {
//		// 1 读取日志
//		select {
//		case line := <- taillog.ReadChan():
//			// 2 发送到kafka
//			kafka.SendToKafka(cfg.KafkaConfig.Topic,line.Text)
//		default:
//			time.Sleep(time.Second)
//		}
//	}
//}

func main(){
	// 0 加载配置文件
	//cfg ,err := ini.Load("./config/config.ini")
	err := ini.MapTo(cfg, "./config/config.ini")
	if err != nil{
		fmt.Println("加载配置文件失败，err : ",err)
		return
	}


	// 1 初始化kafka连接
	err = kafka.Init([]string{cfg.KafkaConfig.Address} , cfg.KafkaConfig.ChanMaxSize)
	if err != nil{
		fmt.Println("初始化kafka失败，err : ",err)
		return
	}
	fmt.Println("初始化kafka成功。")

	// 2 初始化etcd
	err = etcd.Init(cfg.EtcdConfig.Address, time.Duration(cfg.EtcdConfig.Timeout) * time.Second)
	if err != nil{
		fmt.Println("初始化etcd失败，err : ",err)
		return
	}
	fmt.Println("初始化etcd成功。")
	// 为了实现每个logagent都拉取自己独有的配置，所以要以自己的ip地址作为区分
	ipStr, err := utils.GetOutboundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConfig.Key,ipStr)
	// 2.1 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil{
		fmt.Println("etcd.GetConf失败，err : ",err)
		return
	}
	fmt.Println("从etcd获取配置成功。",logEntryConf)

	for index ,value := range  logEntryConf{
		fmt.Println(index,value)
	}

	// 3 收集日志发往kafka
	// 3.1 循环每一个日志收集项，创建TailObj
	taillog.Init(logEntryConf)
	// 因为NewConfChan访问了tskMgr的newConfChan，这个channel是在taillog.Init才初始化
	// 2.2 派一个哨兵监视日志收集项的变化（有变化及时通知logagent实现热加载）
	newConfChan := taillog.NewConfChan()	// 从taillog包中获取对外暴露的通道

	var wg sync.WaitGroup
	wg.Add(1)
	go etcd.WatchConf(etcdConfKey,newConfChan) // 哨兵发现最新的配置信息会通知上面的通道
	wg.Wait()
	//run()


}