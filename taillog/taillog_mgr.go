package taillog

import (
	"fmt"
	"logagent/etcd"
	"time"
)

var tskMgr *taillogMgr

// 管理者
type taillogMgr struct {
	logEntry []*etcd.LogEntry
	tskMap map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}



func Init(logEntryConf []*etcd.LogEntry){
	tskMgr = &taillogMgr{
		logEntry: logEntryConf,	// 把当前的日志收集项配置信息保存起来
		tskMap: make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), // 无缓冲区通道
	}
	// 遍历配置项，有一个配置就起一个NewTailTask任务
	for _, logEntry := range logEntryConf{
		// conf : *etcd.LogEntry
		// logEntry.Path 要收集的日志文件的路径
		// 初始化的时候起了多少个tailtask都要记下来，为了后续判断方便
		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		mk := fmt.Sprintf("%s_%s",logEntry.Path,logEntry.Topic)
		tskMgr.tskMap[mk] = tailObj

	}
	go tskMgr.run()
}

// 监听自己的newConfChan，有了新的配置过来之后做对应的处理

func (t *taillogMgr) run()  {
	for {
		select {
		case newConf := <- t.newConfChan:
			for _, conf := range newConf{
				mk := fmt.Sprintf("%s_%s",conf.Path,conf.Topic)
				_ ,ok := t.tskMap[mk]
				if ok{
					// 原来就有，不需操作
					continue
				}else {
					// 新增的
					tailObj := NewTailTask(conf.Path,conf.Topic)
					t.tskMap[mk] = tailObj
				}
			}
			// 找出原来有t.logEntry有，但是newConf中没有的	，要删除
			for _,c1 := range t.logEntry{ // 从原来的配置中依次拿出配置项
				isDelete := true
				for _, c2 := range newConf{ // 去新的配置中逐一进行比较
					if c2.Path == c1.Path && c2.Topic == c1.Topic{
						isDelete = false
						continue
					}
				}
				if isDelete{
					// 把c1对应的这个tailObj给停掉
					mk := fmt.Sprintf("%s_%s",c1.Path,c1.Topic)
					t.tskMap[mk].cancelFunc()
				}

			}

			// 2 配置删除

			fmt.Println("新的配置来了。",newConf)
		default:
			time.Sleep(time.Second)

		}
	}
}

// 向外暴露tskMgr的newConfChan
func NewConfChan()  chan <- []*etcd.LogEntry{
	return tskMgr.newConfChan

}