package types

import (
	"time"
)

type CrawlTask struct {
	Id         int32
	Domain     string
	Urlpath    string
	Priority   int32
	Status     int32
	CreateTime uint32
	UpdateTime time.Time
}
