package main

import (
	"flag"
	"github.com/jmoiron/sqlx"
	log "github.com/kdar/factorlog"
	"github.com/zhaozhi406/crawler/dao"
	"github.com/zhaozhi406/crawler/utils"
)

func main() {
	var (
		cfgFile string
	)

	flag.StringVar(&cfgFile, "c", "./conf/cfg.ini", "config file")
	flag.Parse()

	config, err := utils.ReadConfig(cfgFile)

	if err == nil {
		db, _ := sqlx.Connect("mysql", config["scheduler"]["dsn"])
		taskDao := dao.InitTaskDao(db)
		crawlRules, _ := taskDao.GetWaitRules()
		for _, rule := range crawlRules {
			log.Infoln(rule.Domain, rule.Urlpath)
		}
	}

}
