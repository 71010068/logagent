package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

var (
	cli *clientv3.Client
)

// 需要收集的日志的配置信息
type LogEntry struct {
	Path string	`json:"path"`	// 日志存放的路径
	Topic string `json:"topic"` // 日志要发往kafka中哪一个topic
}

// 初始化etcd的函数
func Init(addr string , timeout time.Duration) (err error){
	cli , err = clientv3.New(clientv3.Config{
		Endpoints: []string{addr},
		DialTimeout: timeout,
	})
	if err != nil{
		fmt.Println("connect to etcd failed, err : ",err)
		return
	}
	return
}

// 从etcd中根据key获取配置项
func GetConf(key string) (logEntryConf []*LogEntry , err error) {
	// get
	ctx ,cancel := context.WithTimeout(context.Background(),time.Second)
	resp , err := cli.Get(ctx,key)
	cancel()
	if err != nil{
		fmt.Println("get from etcd failed , err : ",err)
		return
	}
	for _ , ev := range resp.Kvs{
		//fmt.Println(ev.Key , ev.Value)
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil{
			fmt.Println("unmarshal etcd value failed , err : ",err)
			return
		}
	}
	return
}

// etcd watch
func WatchConf(key string , newConfCh chan <- []*LogEntry)  {
	ch := cli.Watch(context.Background(),key)
	// 从通道尝试取值（监视的信息）
	for wresp := range ch{
		for _ , evt := range wresp.Events{
			fmt.Println(evt.Type , string(evt.Kv.Key),string(evt.Kv.Value),"-------")
			// 通知taillog.tskMsg
			// 1 判断操作的类型
			var newConf []*LogEntry
			if evt.Type != clientv3.EventTypeDelete{
				// 如果是删除操作，手动传递一个空的配置项
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Println("unmarshal failed , err : ", err)
					continue
				}
			}

			fmt.Println("get new conf ",newConf)
			newConfCh <-newConf

		}
	}
}