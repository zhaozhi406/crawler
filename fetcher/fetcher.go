package fetcher

import (
	"net/http"
	"time"
	"github.com/zhaozhi406/types"
	"github.com/zhaozhi406/utils"
	"github.com/zhaozhi406/lib"
	"encoding/json"
	"sync"
	"strconv"
	log "github.com/kdar/factorlog"
)

type Fetcher struct {
	addr string
	taskQueue chan types.TaskPack
	nWorkers int32
	wg *sync.WaitGroup
	quitChan chan bool
	scheduler_addr string
	scheduler_api map[string]string
	pageStore PageStore
}

const ErrOk = 0
const (
	ErrDataError = 1000 + iota
	ErrInputError
)

var (
	apiNamePathMap = map[string]string {
		"push_tasks": "/push/tasks"
	}
)

func InitFetcher(config map[string]string) *Fetcher {
	taskQueueSize, _ := strconv.Atoi(config["task_queue_size"])
	nWorkers, _ := strconv.Atoi(config["worker_num"])
	addr := config["listen_addr"]
	scheduler_addr := config["scheduler_addr"]
	scheduler_api := map[string]string{}
	json.Unmarshal([]byte(config["scheduler_api"]), &scheduler_api)

	queue := make(chan types.TaskPack, taskQueueSize)
	wg := &sync.WaitGroup{}
	quitChan := make(chan bool, 1)
	pageStore := initPageStore(config)

	return &Fetcher{
		addr           : addr, 
		taskQueue:     : queue, 
		nWorkers       : nWorkers, 
		wg             : wg, 
		quitChan       : quitChan
		scheduler_addr : scheduler_addr,
		scheduler_api  : scheduler_api,
		pageStore      : pageStore
	}
}

func initPageStore(config map[string]string) PageStore {
	localDir := config['local_dir']
	weedfsMaster := config['weedfs_master']
	if localDir != '' {
		return &LocalPageStore{dir: localDir}
	}else if weedfsMaster != nil {
		return nil
	}
	log.Warnln("does not specify local dir or weedfs master! save pages to ./html_pages/")
	return &LocalPageStore{dir: "./html_pages"}
}

//启动Fetcher
func (this *Fetcher) Run() {
	//启动api server
	go this.httpService()
	for i:0; i<this.nWorkers; i++ {
		go this.fetchPage(this.pageStore)
	}

	utils.HandleQuitSignal(func(){
		this.quitChan <- true
	})
	this.wg.Wait()
}

func (this *Fetcher) httpService() {
	mux := http.NewServeMux()

	mux.HandleFunc("/push/tasks", this.pushTasksHandler)
	http.ListenAndServe(this.addr, mux)
}

func (this *Fetcher) pushTasksHandler(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	tasksJson := req.Form.Get('tasks')
	taskPacks := []types.TaskPack{}
	var err error = nil
	var result = types.JsonResult{}
	if tasksJson != '' {
		err = json.Unmarshal(tasksJson[0], &taskPacks)
		if err != nil {
			msg := "Unmarshal task packs error: " + err.Error()
			log.Errorln(msg)
			result.Err = ErrDataError
			result.Msg = msg
		}else{
			//添加任务到队列
			//最多只允许执行1秒钟
			timerChan := time.After(1 * time.Second)
			cnt := 0
			for pack, _ := range taskPacks {
				select {
					case this.taskQueue <- pack:
						cnt++
					case <- timerChan:
						break
				}
			}
			result.Err = ErrOk
			result.Data = taskPacks[:cnt]    //将成功进入队列的任务返回
		}
	}else{
		msg := "missing `tasks` key or has no content in the POST request."
		log.Warnln(msg)
		result.Err = ErrInputError
		result.Msg = msg
	}
	utils.OutputJsonResult(result)
}

func (this *Fetcher) fetchPage(pageStore PageStore) {
	this.wg.Add(1)
	httpClient := lib.HttpClient{}
	for {
		select {
			case taskPack, _ := <- this.taskQueue :
				destUrl := taskPack.Domain + taskPack.Urlpath
				html, err := httpClient.Get(destUrl)
				done := 0
				if err != nil {
					//report success to scheduler, make a log, save html
					html, err = httpClient.IconvHtml(html, 'utf-8')
					done = 1
					log.Infoln("fetch '" + destUrl + "' done.")
					err = pageStore.Save(taskPack.Domain, taskPack.Urlpath, string(html))
					if err != nil {
						log.Errorln("fetcher save ", taskPack.Domain, taskPack.Urlpath, " error:", err)
					}
				}else{
					//report fail to scheduler
					log.Infoln("fetch '" + destUrl + "' failed!")
				}
				//向scheduler报告任务完成情况
				reportUrl := fmt.Sprintf("%s%s?task_id=%d&done=%d", this.scheduler_addr, this.scheduler_api["report"], taskPack.TaskId, done)
				res, err := httpClient.Get(reportUrl)
				if err != nil {
					log.Errorln("report ", reportUrl, " failed!")
				}else{
					result := types.JsonResult{}
					err = json.Unmarshal(res, &result)
					if err != nil || result.Err != 0 {
						log.Errorln("report ", reportUrl, ", get error response: ", string(res))
					}
				}
			case <- this.quitChan :
				log.Infoln("quit fetch page...")
				break
	}
	this.wg.Done()
}
