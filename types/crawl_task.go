package types

import (
	"time"
)

type CrawlTask struct {
	Id            int32
	Domain        string
	Urlpath       string
	Priority      int32
	Cycle         int32
	Status        int32
	LastCrawlTime int64     `db:"last_crawl_time"`
	CrawlTimes    int32     `db:"crawl_times"`
	CreateTime    time.Time `db:"create_time"`
	UpdateTime    time.Time `db:"update_time"`
}
