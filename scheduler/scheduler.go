package scheduler

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/kdar/factorlog"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/zhaozhi406/crawler/dao"
	"github.com/zhaozhi406/crawler/lib"
	"github.com/zhaozhi406/crawler/types"
	"github.com/zhaozhi406/crawler/utils"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Scheduler struct {
	fetchRulesPeriod time.Duration
	fetchTasksPeriod time.Duration
	listenAddr       string
	db               *sqlx.DB
	taskDao          *dao.TaskDao
	fetchers         []string
	fetcherApi       map[string]string
	politeVisitor    *PoliteVisitor
	redisPool        *pool.Pool
	redisPoolSize    int
	redisHeartbeat   int
	quitChan         chan bool
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
	fetcherApi := map[string]string{}
	json.Unmarshal([]byte(config["fetcher_api"]), &fetcherApi)
	minHostVisitInterval, _ := strconv.Atoi(config["min_host_visit_interval"])
	redisAddr := config["redis_addr"]
	redisPoolSize, _ := strconv.Atoi(config["redis_pool_size"])
	redisHeartbeat, _ := strconv.Atoi(config["redis_heartbeat"])

	pool, err := pool.New("tcp", redisAddr, redisPoolSize)
	if err != nil {
		log.Errorln("init redis pool error: ", err)
		return nil
	}
	politeVisitor := InitPoliteVisitor(pool, int64(minHostVisitInterval))

	quitChan := make(chan bool, 1)

	return &Scheduler{
		fetchRulesPeriod: fetchRulesPeriod,
		fetchTasksPeriod: fetchTasksPeriod,
		listenAddr:       listenAddr,
		db:               db,
		taskDao:          taskDao,
		fetchers:         fetchers,
		fetcherApi:       fetcherApi,
		politeVisitor:    politeVisitor,
		redisPool:        pool,
		redisPoolSize:    redisPoolSize,
		redisHeartbeat:   redisHeartbeat,
		quitChan:         quitChan}
}

func (this *Scheduler) Run() {
	go this.httpService()

	//a cronjob wrapper for add tasks
	f := func(dummy ...interface{}) {
		this.AddTasksFromRules()
	}
	cronJob := lib.InitCronJob(f, nil, this.fetchRulesPeriod)
	go cronJob.Run()

	//a cronjob wrapper for DispatchTasks
	f1 := func(dummy ...interface{}) {
		this.DispatchTasks()
	}
	cronJob1 := lib.InitCronJob(f1, nil, this.fetchTasksPeriod)
	go cronJob1.Run()

	go this.redisPool.KeepAlive(this.redisHeartbeat)

	go utils.HandleQuitSignal(func() {
		close(this.quitChan)
	})

	<-this.quitChan
}

//分发task给Fetcher
func (this *Scheduler) DispatchTasks() {
	//获取等待任务
	tasks, err := this.FetchTasks()
	if err != nil {
		log.Errorln("[DispatchTasks] fetch tasks error: ", err)
		return
	}
	if len(tasks) == 0 {
		log.Warnln("[DispatchTasks] no wait tasks yet.")
		return
	}
	//排序
	sorter := lib.CrawlTaskSorter{Now: time.Now().Unix()}
	sorter.Sort(tasks, nil)
	//post到fetchers
	picked := map[int32]bool{}
	httpClient := lib.HttpClient{}
	for _, fetcher := range this.fetchers {
		taskPacks := []types.TaskPack{}
		//挑选未分配的，且符合礼貌原则的任务
		for _, task := range tasks {
			_, ok := picked[task.Id]
			if !ok && this.politeVisitor.IsPolite(task.Domain, fetcher) {
				taskPacks = append(taskPacks, types.TaskPack{TaskId: task.Id, Domain: task.Domain, Urlpath: task.Urlpath})
				picked[task.Id] = true
				//缓存最后访问时间，实际有误差，但是实现简单
				this.politeVisitor.SetLastVisitTime(task.Domain, fetcher, time.Now().Unix())
			}
		}
		jsonBytes, err := json.Marshal(taskPacks)
		if err != nil {
			log.Errorln("make task packs error: ", err)
		} else {
			param := url.Values{}
			param.Add("tasks", string(jsonBytes))
			result, err := httpClient.Post("http://"+fetcher+this.fetcherApi["push_tasks"], param)
			if err != nil {
				log.Errorln("post task packs to fetcher:", fetcher, ", error:", err, " data:", string(jsonBytes))
			} else {
				jsonResult := types.JsonResult{}
				err = json.Unmarshal(result, &jsonResult)
				if err != nil {
					log.Errorln("json unmarshal error:", err, " data:", string(result))
				} else {
					log.Infoln("get push tasks response: ", jsonResult)
				}
			}
		}
	}
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
	_, err := utils.CheckHttpParams(req, requiredParams)
	result := types.JsonResult{}
	if err != nil {
		log.Errorln(err)
		result.Err = ErrInputError
		result.Msg = err.Error()
		utils.OutputJsonResult(w, result)
		return
	}

	taskId := req.Form.Get("task_id")
	intTaskId, _ := strconv.Atoi(taskId)
	task := types.CrawlTask{Id: int32(intTaskId)}
	tasks := []types.CrawlTask{task}
	done := req.Form.Get("done")
	var status dao.TaskStatus
	if done == "1" {
		status = dao.TASK_FINISH
	} else {
		status = dao.TASK_FAILED
	}
	_, err = this.taskDao.SetTasksStatus(tasks, status)
	if err != nil {
		msg := fmt.Sprintf("set task %s status to %d, error: %v", taskId, status, err)
		log.Errorln(msg)
		result.Err = ErrDbError
		result.Msg = msg

	} else {
		log.Errorln("set task ", taskId, " status to ", status, " finished.")
		result.Err = ErrOk
	}
	utils.OutputJsonResult(w, result)
}
