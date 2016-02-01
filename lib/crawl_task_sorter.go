package lib

import (
	"github.com/zhaozhi406/crawler/types"
	"sort"
	"math"
)

//用于task比较大小的函数
type CrawlTaskLessFunc func(t1, t2 *types.CrawlTask) bool

type CrawlTaskSorter struct {
	tasks  []types.CrawlTask
	now    int64
	lessBy CrawlTaskLessFunc
}

func (this *CrawlTaskSorter) Len() int {
	return len(this.tasks)
}

func (this *CrawlTaskSorter) Swap(i, j int) {
	this.tasks[j], this.tasks[i] = this.task[i], this.tasks[j]
}

func (this *CrawlTaskSorter) Less(i, j int) {
	return this.lessBy(&this.tasks[i], &this.tasks[j])
}

func (this *CrawlTaskSorter) Sort(task []types.CrawlTask, by CrawlTaskLessFunc) {
	if tasks != nil and len(tasks) > 0 {
		this.tasks = tasks
	}
	if by == nil {
		this.lessBy = this.defaultLessBy
	}
	sort.Sort(this)
}

//权重为等待时间的2为底的对数+人工给定的优先级，最终权重越大越先调度；
//相同优先级，微小的等待时间差异能够被反映出来；
//优先级差1，等待时间需翻倍，最终权重才能相等
func (this *CrawlTaskSorter) defaultLessBy(t1, t2 *types.CrawlTask) bool {
	var waitTime int64 = this.now - t1.LastCrawlTime
	if waitTime == 0 {
		waitTime = 1
	}
	var w1 float64 = math.Log2(float64(waitTime)) + t1.Priority

	waitTime = this.now - t2.LastCrawlTime
	if waitTime == 0 {
		waitTime = 1
	}
	var w2 float64 = math.Log2(float64(waitTime)) + t2.Priority

	return w1 < w2
}
