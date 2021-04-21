// 专门从日志文件收集日志的模块
package taillog

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"logagent/kafka"
)

// 一个具体的日志收集的任务
type TailTask struct {
	path string
	topic string
	instance *tail.Tail	 // 打开的文件
	// 为了能退出t.run()
	ctx context.Context
	cancelFunc context.CancelFunc
}



// 构造函数
func NewTailTask(path ,topic string)(tailObj *TailTask){
	ctx ,cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path: path,
		topic: topic,
		ctx: ctx,
		cancelFunc: cancel,
	}
	tailObj.init()	// 根据路径去打开对应的日志
	return
}

func (t *TailTask) init(){
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件哪个地方开始读
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,
	}
	var err error
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail 文件失败，err : ", err)
	}

	// 当goroutine执行的函数退出的时候，goroutine就结束了
	go t.run() // 直接去采集日志发送到kafka
}
func (t *TailTask)run()  {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Println("tail task 结束了：",t.path+"_"+t.topic)
			return
		case line := <- t.instance.Lines : // 从tailObj的通道中一行一行的读取日志数据
			// 3.2 发往kafka
			// kafka.SendToKafka(t.topic,line.Text)  // 函数调函数
			// 先把日志数据发到一个通道中
			kafka.SendToChan(t.topic,line.Text)
			// kafka那个包中有单独的goroutine去取日志数据发到kafka
		}
	}
}
