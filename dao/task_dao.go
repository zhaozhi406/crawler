package dao

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/kdar/factorlog"
	"github.com/zhaozhi406/crawler/types"
	"time"
)

type TaskDao struct {
	db *sqlx.DB
}

type TaskStatus int32
type RuleStatus int32

const (
	RuleTable = "crawl_rules"
	TaskTable = "crawl_tasks"
)

const (
	TASK_CANCELED TaskStatus = -1 + iota
	TASK_WAITING
	TASK_CRAWLING
	TASK_FINISH
	TASK_FAILED
)

const (
	RULE_PAUSE RuleStatus = -1 + iota
	RULE_NORMAL
	RULE_ADDED
)

var (
	ErrNotEqual = errors.New("not equal!")
	ErrNoTasks  = errors.New("no tasks!")
)

func InitTaskDao(db *sqlx.DB) *TaskDao {
	return &TaskDao{db: db}
}

/*
	从规则库读取下一批需调度的规则
*/
func (this *TaskDao) GetWaitRules() ([]types.CrawlRule, error) {
	crawlRules := []types.CrawlRule{}
	sqlStr := fmt.Sprintf("select * from %s where status = 0", RuleTable)
	err := this.db.Select(&crawlRules, sqlStr)
	if err != nil {
		log.Errorln(err)
	}
	return crawlRules, err
}

func (this *TaskDao) AddNewTasks(tasks []types.CrawlTask) (int64, []sql.Result, error) {
	tx, err := this.db.Beginx()
	if err != nil {
		log.Errorln("begin transaction error:", err)
		return 0, nil, err
	}

	results := make([]sql.Result, len(tasks))
	var affectedRows int64 = 0
	sqlStr := fmt.Sprintf("insert into %s (domain, urlpath, priority, cycle, status, last_crawl_time, crawl_times, create_time, update_time) values (:domain, :urlpath, :priority, :cycle, :status, :last_crawl_time, :crawl_times, :create_time, :update_time) on duplicate key update priority=values(priority), cycle=values(cycle), update_time=values(update_time) ", TaskTable)
	for i, task := range tasks {
		result, err1 := tx.NamedExec(sqlStr, task)
		results[i] = result
		if err1 != nil {
			err = err1
			log.Errorln("transaction error:", err, " data:", task)
		} else {
			affectedRows++
		}

	}
	err = tx.Commit()
	return affectedRows, results, err
}

/*
	根据任务添加结果，修改rule的状态
*/
func (this *TaskDao) UpdateRules(rules []types.CrawlRule, taskAddedResults []sql.Result) (int64, error) {
	if len(taskAddedResults) == 0 {
		log.Errorln("no tasks added so no need to update rules!")
		return 0, ErrNoTasks
	}
	if len(rules) != len(taskAddedResults) {
		log.Errorln("rules number not equal to added tasks number!")
		return 0, ErrNotEqual
	}

	ruleIds := []int32{}
	var insertId int64
	for i, result := range taskAddedResults {
		insertId, _ = result.LastInsertId()
		if insertId > 0 {
			ruleIds = append(ruleIds, rules[i].Id)
		}
	}
	sqlTmp := fmt.Sprintf("update %s set status=%d, update_time='%s' where id in (?)", RuleTable, RULE_ADDED, time.Now().Format("2006-01-02 15:04:05"))

	sqlStr, args, err := sqlx.In(sqlTmp, ruleIds)
	if err != nil {
		log.Errorln("make in sql error: ", err)
		return 0, err
	}

	var result sql.Result
	var affectedRows int64
	result, err = this.db.Exec(sqlStr, args...)

	if err != nil {
		log.Errorln("update rules error: ", err)
		return 0, err
	}
	affectedRows, _ = result.RowsAffected()
	return affectedRows, nil
}

/*
	设置任务状态
*/
func (this *TaskDao) SetTasksStatus(tasks []types.CrawlTask, status TaskStatus) (int64, error) {
	nTasks := len(tasks)
	var affectedRows int64 = 0
	var err = ErrNoTasks
	if nTasks > 0 {
		taskIds := []int32{}
		var result sql.Result
		var args []interface{}
		var sqlStr string
		var now = time.Now()

		for _, task := range tasks {
			taskIds = append(taskIds, task.Id)
		}

		if status == TASK_FINISH {
			sqlStr = fmt.Sprintf("update %s set status=%d, last_crawl_time=%d, crawl_times=crawl_times+1, update_time='%s' where id in (?)", TaskTable, status, now.Unix(), now.Format("2006-01-02 15:04:05"))
		} else {
			sqlStr = fmt.Sprintf("update %s set status=%d, update_time='%s' where id in (?)", TaskTable, status, now.Format("2006-01-02 15:04:05"))
		}

		sqlStr, args, err = sqlx.In(sqlStr, taskIds)
		if err != nil {
			log.Errorln("build sql to set task status failed! sql is ", sqlStr)
		} else {
			result, err = this.db.Exec(sqlStr, args...)
			if err != nil {
				log.Errorln("update tasks status error: ", err)
			}

		}
		affectedRows, _ = result.RowsAffected()
	}
	return affectedRows, err
}

/*
	选取status为0, 或status=2且调度时间已到的任务
*/
func (this *TaskDao) GetWaitingTasks() ([]types.CrawlTask, error) {
	crawlTasks := []types.CrawlTask{}

	sqlStr := fmt.Sprintf("select * from %s where status=%d or (status=%d and cycle+last_crawl_time <= %d)", TaskTable, TASK_WAITING, TASK_FINISH, time.Now().Unix())

	err := this.db.Select(&crawlTasks, sqlStr)
	if err != nil {
		log.Errorln(err)
	}
	return crawlTasks, err
}

/*
	规则转为任务
*/
func (this *TaskDao) ConvertRuleToTask(rule types.CrawlRule) types.CrawlTask {
	tm := time.Now()
	task := types.CrawlTask{Domain: rule.Domain, Urlpath: rule.Urlpath, Priority: rule.Priority, Cycle: rule.Cycle, Status: 0, LastCrawlTime: 0, CrawlTimes: 0, CreateTime: tm, UpdateTime: tm}
	return task
}
