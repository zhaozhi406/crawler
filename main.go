package main

import (
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	log "github.com/kdar/factorlog"
	"github.com/zhaozhi406/crawler/fetcher"
	"github.com/zhaozhi406/crawler/scheduler"
	"github.com/zhaozhi406/crawler/utils"
)

func main() {
	var (
		cfgFile string
		role    string
	)

	flag.StringVar(&cfgFile, "c", "./conf/cfg.ini", "config file")
	flag.StringVar(&role, "r", "", "server role: scheduler, fetcher etc.")
	flag.Parse()

	config, err := utils.ReadConfig(cfgFile)

	if err == nil {
		if role == "scheduler" {
			db, _ := sqlx.Connect("mysql", config["scheduler"]["dsn"])
			log.Infoln("db stats: ", db.Stats())

			scheduler := scheduler.InitScheduler(db, config["scheduler"])
			scheduler.Run()
		} else if role == "fetcher" {
			fetcher := fetcher.InitFetcher(config["fetcher"])
			fetcher.Run()
		} else {
			fmt.Println("unknown role:", role)
		}
	}

}
