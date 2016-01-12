package types

import (
	"time"
)

type CrawlRule struct {
	Id            int32
	Domain        string
	Urlpath       string
	Xpath         string
	Cycle         int32
	NextCrawlTime int32     `db:"next_crawl_time"`
	CreateTime    time.Time `db:"create_time"`
	UpdateTime    time.Time `db:"update_time"`
	Status        int32
}
