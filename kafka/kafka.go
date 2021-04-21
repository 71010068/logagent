// 专门往kafka写日志的模块
package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

type logData struct {
	topic string
	data string
}

var (
	client sarama.SyncProducer // 定义一个全局的连接kafka的生产者客户端
	logDataChan chan *logData
)



// 初始化kafka client连接
func Init(addr []string ,maxSize int) (err error) {
	config := sarama.NewConfig()
	// tailf包使用
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                    // 成功交付的信息将在success channel 返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(addr, config)
	if err != nil {
		fmt.Println("连接kafka新的生产者失败，err : ", err)
		return err
	}
	// 初始化lodDataChan
	logDataChan = make(chan *logData , maxSize)
	// 开启后台的goroutine从通道中取数据发往kafka
	go sendToKafka()
	return
}

// 给外部暴露的一个函数，该函数只把日志数据发送到一个内部的chanel中
func SendToChan(topic, data string){
	msg := &logData{
		topic: topic,
		data: data,
	}
	logDataChan <- msg
}

// 真正往kafka发送日志的函数
func sendToKafka() {
	for  {
		select {
		case ld := <- logDataChan:
			// 构建一个消息
			msg := &sarama.ProducerMessage{}
			msg.Topic = ld.topic
			msg.Value = sarama.StringEncoder(ld.data)

			// 发送到kafka
			pid ,offset ,err := client.SendMessage(msg)
			fmt.Println("------------------")
			if err != nil{
				fmt.Println("发送消息失败，err : ",err)
				return
			}
			fmt.Println(pid,offset)
		default:
			time.Sleep(time.Millisecond*50)
		}
	}
}
