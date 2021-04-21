// 专门往kafka写日志的模块
package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
)

var (
	client sarama.SyncProducer // 定义一个全局的连接kafka的生产者客户端
)

// 初始化kafka client连接
func Init(addr []string ) (err error) {
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
	return
}

func SendToKafka(topic, data string) {
	// 构建一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.StringEncoder(data)

	// 发送到kafka
	pid ,offset ,err := client.SendMessage(msg)
	fmt.Println("------------------")
	if err != nil{
		fmt.Println("发送消息失败，err : ",err)
		return
	}
	fmt.Println(pid,offset)
}
