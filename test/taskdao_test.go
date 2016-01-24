package test

import (
	"flag"

	"github.com/jmoiron/sqlx"
	log "github.com/kdar/factorlog"
	"github.com/zhaozhi406/crawler/dao"
	"github.com/zhaozhi406/crawler/types"
	"github.com/zhaozhi406/crawler/utils"
)

func test(config map[string]map[string]string) {
	db, _ := sqlx.Connect("mysql", config["scheduler"]["dsn"])
	log.Infoln("db stats: ", db.Stats())

	taskDao := dao.InitTaskDao(db)
	crawlRules, _ := taskDao.GetWaitRules()
	crawlTasks := []types.CrawlTask{}
	for _, rule := range crawlRules {
		log.Infoln(rule.Domain, rule.Urlpath)
		crawlTasks = append(crawlTasks, taskDao.ConvertRuleToTask(rule))
	}

	log.Infoln("get task: ", len(crawlTasks))

	num, results, err := taskDao.AddNewTasks(crawlTasks)

	log.Infoln("add task: ", num, results, err)

	affectedRows, _ := taskDao.UpdateRules(crawlRules, results)

	log.Infoln("update rules: ", affectedRows)

	waitingTasks, _ := taskDao.GetWaitingTasks()

	for _, task := range waitingTasks {
		log.Infoln(task.Domain, task.Urlpath)
	}

	affectedRows, _ = taskDao.SetTasksStatus(waitingTasks, dao.TASK_FINISH)
	log.Infoln("set task status: ", affectedRows)

}
