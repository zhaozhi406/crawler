package scheduler

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/kdar/factorlog"
	"github.com/zhaozhi406/crawler/dao"
	"github.com/zhaozhi406/crawler/lib"
	"github.com/zhaozhi406/crawler/types"
	"net/http"
	"strconv"
	"strings"
	"time"
	"encoding/json"
	"net/url"
)

type Scheduler struct {
	fetchRulesPeriod     time.Duration
	fetchTasksPeriod     time.Duration
	listenAddr           string
	db                   *sqlx.DB
	taskDao              *dao.TaskDao
	hostLastVisit        map[string]int64 //上一次访问一个host的时间戳，实际是任务推送给Fetcher的时间
	minHostVisitInterval int              //连续访问同一host的最小时间间隔
	fetchers             []string
	fetcherApi          map[string]string
}

const ErrOk = 0

const (
	ErrDataError = 1000 + iota
	ErrInputError
)

const (
	ErrDbError = 2000 + iota
)

func InitScheduler(db *sqlx.DB, config map[string]string) *Scheduler {
	taskDao := dao.InitTaskDao(db)
	seconds, _ := strconv.Atoi(config["fetch_rules_period"])
	fetchRulesPeriod := time.Duration(seconds) * time.Second
	seconds, _ = strconv.Atoi(config["fetch_tasks_period"])
	fetchTasksPeriod := time.Duration(seconds) * time.Second
	listenAddr := config["listen_addr"]
	fetchers := strings.Split(strings.Replace(config["fetchers"], " ", "", -1), ",")
	fetcherApi := map[string]string
	json.Unmarshal(config["fetcher_api"], &fetcherApi)
	minHostVisitInterval, _ := strconv.Atoi(config["min_host_visit_interval"])

	return &Scheduler{
		fetchRulesPeriod     : fetchRulesPeriod, 
		fetchTasksPeriod     : fetchTasksPeriod, 
		listenAddr           : listenAddr,
		db                   : db, 
		taskDao              : taskDao, 
		minHostVisitInterval : minHostVisitInterval,		
		fetchers             : fetchers,
		fetcherApi           : fetcherApi
	}
}

func (this *Scheduler) Run() {
	go this.httpService()

	//a cronjob wrapper
	f := func(dummy ...interface{}) {
		this.AddTasksFromRules()
	}
	cronJob := lib.InitCronJob(f, nil, this.fetchRulesPeriod)
	go cronJob.Run()

}

//分发task给Fetcher
func (this *Scheduler) DispatchTasks() {
	//获取等待任务
	tasks, err := this.FetchTasks()
	if err != nil {
		log.Errorln("[DispatchTasks] fetch tasks error: ", err)
		return
	}
	//排序
	sorter := lib.CrawlTaskSorter{now: time.Now().Unix()}
	sorter.Sort(tasks, nil)
	//post到fetchers
	picked := map[int32]bool{}
	httpClient := lib.HttpClient{}
	for fetcher, _ := range this.fetchers {
		taskPacks := types.TaskPack{}
		//挑选符合礼貌原则的任务
		for task, _ := range tasks {
			_, ok := picked[task.TaskId]
			if ok && this.isPoliteVisit(fetcher, task.Domain) {
				taskPacks = append(taskPacks, types.TaskPack{TaskId: task.TaskId, Domain:task.Domain, Urlpath:task.Urlpath})
				picked[task.TaskId] = true
			}
		}
		jsonBytes, err := json.Marshal(taskPacks)
		if err != nil {
			log.Errorln("make task packs error: ", err)
		}else{
			param := url.Values{}
			param.Add("tasks", string(jsonBytes))
			result, err := httpClient.Post(fetcher + this.fetcherApi["push_tasks"], param)
			if err != nil {
				log.Errorln("post task packs to fetcher:", fetcher, " error:", err, " data:", string(jsonBytes))
			}else{
				jsonResult := types.JsonResult{}
				err = json.Unmarshal(result, &jsonResult)
				if err != nil {
					log.Errorln("json unmarshal error:", err, " data:", string(result))
				}else{
					log.Infoln("get push tasks response: ", jsonResult)
				}
			}
		}
	}
}

//是否符合礼貌原则，即避免对同一站点访问过于频繁
func (this *Scheduler) isPoliteVisit(fetcher_addr string, domain string) bool {
	parts := strings.Split(fetcher_addr, ":")
	log.Infoln(parts[0])
	return true
}
/*
	获取等待调度的任务
*/
func (this *Scheduler) FetchTasks() ([]types.CrawlTask, error) {

	waitingTasks, err := this.fetchTaskFromDb()
	if err != nil {
		log.Errorln("fetch tasks error: ", err)
	}
	return waitingTasks, err
}

/*
	从数据库获取等待调度的任务
*/
func (this *Scheduler) fetchTaskFromDb() ([]types.CrawlTask, error) {

	waitingTasks, _ := this.taskDao.GetWaitingTasks()

	return waitingTasks, nil
}

/*
	从规则库导入任务
*/
func (this *Scheduler) AddTasksFromRules() {

	crawlRules, err := this.taskDao.GetWaitRules()
	nRules := len(crawlRules)

	if err != nil {
		log.Errorln("get wait rules error: ", err)
	}

	if nRules > 0 {
		crawlTasks := []types.CrawlTask{}
		for _, rule := range crawlRules {
			crawlTasks = append(crawlTasks, this.taskDao.ConvertRuleToTask(rule))
		}

		log.Infoln("get tasks from rules: ", len(crawlTasks))

		num, results, _ := this.taskDao.AddNewTasks(crawlTasks)

		log.Infoln("add new tasks: ", num)

		affectedRows, _ := this.taskDao.UpdateRules(crawlRules, results)

		log.Infoln("update rules: ", affectedRows)
	} else {
		log.Infoln("no waiting rules yet.")
	}
}

func (this *Scheduler) httpService() {
	mux := http.NewServeMux()
	mux.HandleFunc("/report/task", this.reportTaskHandler)
	http.ListenAndServe(this.listenAddr, mux)
}

func (this *Scheduler) reportTaskHandler(w http.ResponseWriter, req *http.Request) {
	requiredParams := map[string]string{"task_id": "int", "done": "int"}
	param, err := utils.CheckHttpParams(req, requiredParams)
	result := types.JsonResult{}
	if err != nil {
		log.Errorln(err)
		result.Err = errInputError
		result.Msg = err.Error()
		utils.OutputJsonResult(result)
		return
	}

	taskId := req.Form.Get("task_id")
	intTaskId, _ := strconv.Atoi(taskId)
	task := types.CrawlTask{TaskId: intTaskId}
	tasks := []types.CrawlTask{task}
	done := req.Form.Get("done")
	var status dao.TaskStatus
	if done == '1' {
		status = dao.TASK_FINISH
	} else {
		status = dao.TASK_FAILED
	}
	n, err := this.taskDao.SetTasksStatus(tasks, status)
	if err != nil {
		msg := "set task " + taskId + " status to " + status + " error: " + err.Error()
		log.Errorln(msg)
		result.Err = ErrDbError
		result.Msg = msg

	} else {
		log.Errorln("set task ", taskId, " status to ", status, " finished.")
		result.Err = ErrOk
	}
	utils.OutputJsonResult(result)
}
