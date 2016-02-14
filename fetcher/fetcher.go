package fetcher

import (
	"encoding/json"
	"fmt"
	log "github.com/kdar/factorlog"
	"github.com/zhaozhi406/crawler/lib"
	"github.com/zhaozhi406/crawler/types"
	"github.com/zhaozhi406/crawler/utils"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Fetcher struct {
	addr           string
	taskQueue      chan types.TaskPack
	nWorkers       int
	wg             *sync.WaitGroup
	quitChan       chan bool
	scheduler_addr string
	scheduler_api  map[string]string
	pageStore      PageStore
}

const ErrOk = 0
const (
	ErrDataError = 1000 + iota
	ErrInputError
)

var (
	apiNamePathMap = map[string]string{
		"push_tasks": "/push/tasks"}
)

func InitFetcher(config map[string]string) *Fetcher {
	taskQueueSize, _ := strconv.Atoi(config["task_queue_size"])
	nWorkers, _ := strconv.Atoi(config["workers_num"])
	addr := config["listen_addr"]
	scheduler_addr := config["scheduler"]
	scheduler_api := map[string]string{}
	json.Unmarshal([]byte(config["scheduler_api"]), &scheduler_api)

	queue := make(chan types.TaskPack, taskQueueSize)
	wg := &sync.WaitGroup{}
	quitChan := make(chan bool, 1)
	pageStore := initPageStore(config)

	return &Fetcher{
		addr:           addr,
		taskQueue:      queue,
		nWorkers:       nWorkers,
		wg:             wg,
		quitChan:       quitChan,
		scheduler_addr: scheduler_addr,
		scheduler_api:  scheduler_api,
		pageStore:      pageStore}
}

func initPageStore(config map[string]string) PageStore {
	localDir := config["local_dir"]
	weedfsMaster := config["weedfs_master"]
	if localDir != "" {
		return &LocalPageStore{dir: localDir}
	} else if weedfsMaster != "" {
		return nil
	}
	log.Warnln("does not specify local dir or weedfs master! save pages to ./html_pages/")
	return &LocalPageStore{dir: "./html_pages"}
}

//启动Fetcher
func (this *Fetcher) Run() {
	//启动api server
	go this.httpService()
	for i := 0; i < this.nWorkers; i++ {
		this.wg.Add(1)
		go this.fetchPage(this.pageStore)
	}
	log.Infoln("start ", this.nWorkers, " fetch workers...")

	go utils.HandleQuitSignal(func() {
		close(this.quitChan)
	})

	this.wg.Wait()
}

func (this *Fetcher) httpService() {
	mux := http.NewServeMux()

	mux.HandleFunc("/push/tasks", this.pushTasksHandler)
	http.ListenAndServe(this.addr, mux)
}

func (this *Fetcher) pushTasksHandler(w http.ResponseWriter, req *http.Request) {
	log.Debugln("get request: ", req.RemoteAddr, req.URL)
	req.ParseForm()
	tasksJson := req.Form.Get("tasks")
	taskPacks := []types.TaskPack{}
	var err error = nil
	var result = types.JsonResult{}
	if tasksJson != "" {
		err = json.Unmarshal([]byte(tasksJson), &taskPacks)
		if err != nil {
			msg := "Unmarshal task packs error: " + err.Error()
			log.Errorln(msg)
			result.Err = ErrDataError
			result.Msg = msg
		} else {
			//添加任务到队列
			//最多只允许执行1秒钟
			timerChan := time.After(1 * time.Second)
			cnt := 0
			for _, pack := range taskPacks {
				select {
				case this.taskQueue <- pack:
					cnt++
				case <-timerChan:
					break
				}
			}
			result.Err = ErrOk
			result.Data = taskPacks[:cnt] //将成功进入队列的任务返回
		}
	} else {
		msg := "missing `tasks` key or has no content in the POST request."
		log.Warnln(msg)
		result.Err = ErrInputError
		result.Msg = msg
	}
	utils.OutputJsonResult(w, result)
}

func (this *Fetcher) fetchPage(pageStore PageStore) {
	defer this.wg.Done()

	httpClient := lib.HttpClient{}
loop:
	for {
		select {
		case taskPack := <-this.taskQueue:
			destUrl := taskPack.Domain + taskPack.Urlpath
			html, err := httpClient.Get(destUrl)
			log.Debugln("goto fetch ", destUrl)
			done := 0
			if err == nil {
				//report success to scheduler, make a log, save html
				html, err = httpClient.IconvHtml(html, "utf-8")
				done = 1
				log.Infoln("fetch '" + destUrl + "' done.")
				err = pageStore.Save(taskPack.Domain, taskPack.Urlpath, string(html))
				if err != nil {
					log.Errorln("fetcher save ", taskPack.Domain, taskPack.Urlpath, " error:", err)
				}
			} else {
				//report fail to scheduler
				log.Errorln("fetch '"+destUrl+"' failed!", err)
			}
			//向scheduler报告任务完成情况
			reportUrl := fmt.Sprintf("http://%s%s?task_id=%d&done=%d", this.scheduler_addr, this.scheduler_api["report"], taskPack.TaskId, done)
			res, err := httpClient.Get(reportUrl)
			if err != nil {
				log.Errorln("report ", reportUrl, " failed!")
			} else {
				result := types.JsonResult{}
				err = json.Unmarshal(res, &result)
				if err != nil || result.Err != 0 {
					log.Errorln("report ", reportUrl, ", get error response: ", string(res))
				}
			}
		case <-this.quitChan:
			//this.quitChan should be closed somewhere
			log.Infoln("quit fetch page...")
			break loop
		}
	}
}
