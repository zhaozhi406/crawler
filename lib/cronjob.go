package utils

import (
	"time"
)

type CronJobFunc func(...interface{})

type CronJob struct {
	quitChan chan bool     //用于处理退出的chan
	f        CronJobFunc   //要执行的函数
	args     []interface{} //函数的参数
	period   time.Duration //周期，秒
}

func InitCronJob(f CronJobFunc, args []interface{}, period time.Duration) *CronJob {
	quitChan := make(chan bool)
	return &CronJob{quitChan: quitChan, f: f, args: args, period: period}
}

func (this *CronJob) Run() {

	var timerChan = time.After(this.period)

loop:
	for {
		select {
		case <-this.quitChan:
			break loop
		case <-timerChan:
			this.f(this.args)
			timerChan = time.After(this.period)
		}
	}
}

func (this *CronJob) Stop() {
	close(this.quitChan)
}
