package dao

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/kdar/factorlog"
	"github.com/zhaozhi406/crawler/types"
)

type TaskDao struct {
	db *sqlx.DB
}

var (
	RuleTable = "crawl_rules"
	TaskTable = "crawl_tasks"
)

func InitTaskDao(db *sqlx.DB) *TaskDao {
	return &TaskDao{db}
}

/*
	从规则库读取下一批需调度的规则
*/
func (this *TaskDao) GetWaitRules() ([]types.CrawlRule, error) {
	crawlRules := []types.CrawlRule{}
	ts := time.Now().Unix()
	sqlStr := fmt.Sprintf("select * from %s where next_crawl_time <= %d and status = 0", RuleTable, ts)
	err := this.db.Select(&crawlRules, sqlStr)
	if err != nil {
		log.Errorln(err)
	}
	return crawlRules, err
}
