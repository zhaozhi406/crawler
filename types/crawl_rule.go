package types

import (
	"time"
)

type CrawlRule struct {
	Id         int32
	Domain     string
	Urlpath    string
	Xpath      string
	Cycle      int32
	Priority   int32
	CreateTime time.Time `db:"create_time"`
	UpdateTime time.Time `db:"update_time"`
	Status     int32
}
