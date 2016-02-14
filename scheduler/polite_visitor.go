package scheduler

import (
	"fmt"
	log "github.com/kdar/factorlog"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"strings"
	"time"
)

type PoliteVisitor struct {
	pool                 *pool.Pool
	minHostVisitInterval int64 //连续访问同一host的最小时间间隔
}

func InitPoliteVisitor(pool *pool.Pool, minVisitInterval int64) *PoliteVisitor {
	return &PoliteVisitor{pool: pool, minHostVisitInterval: minVisitInterval}
}

//a convenient wrapper
func (this *PoliteVisitor) IsPolite(domain string, hostname string) bool {
	return time.Now().Unix()-this.GetLastVisitTime(domain, hostname) >= this.minHostVisitInterval
}

//hget hostname domain
func (this *PoliteVisitor) GetLastVisitTime(domain string, hostname string) int64 {
	client, err := this.pool.Get()
	defer this.pool.Put(client)
	var ret int64 = -1
	if err != nil {
		log.Errorln("get redis client error: ", err)
	} else {
		host := this.canonicalHostname(hostname)
		dm := this.canonicalDomain(domain)
		key := this.makeRedisKey(host)
		resp := client.Cmd("hget", key, dm)
		if !resp.IsType(redis.Nil) {
			ts, err := resp.Int64()
			if err != nil {
				log.Debugln("convert redis response to int64 error: ", err, " resp:", resp)
			} else {
				ret = ts
			}
		} else {
			log.Debugln("hget ", key, "->", dm, " return nil")
		}
	}
	return ret
}

//hset hostname domain ts
func (this *PoliteVisitor) SetLastVisitTime(domain string, hostname string, ts int64) error {
	client, err := this.pool.Get()
	defer this.pool.Put(client)
	if err != nil {
		log.Errorln("get redis client error: ", err)
	} else {
		host := this.canonicalHostname(hostname)
		dm := this.canonicalDomain(domain)
		key := this.makeRedisKey(host)
		var n int64
		resp := client.Cmd("hset", key, dm, ts)
		n, err = resp.Int64()
		if err != nil {
			log.Errorln("hset ", key, " ", dm, " ", ts, " error: ", err)
		} else {
			log.Debugln("hset ", key, " ", dm, " ", ts, " updated: ", n)
		}
	}
	return err
}

//remove port part, only ip matters
func (this *PoliteVisitor) canonicalHostname(hostname string) string {
	parts := strings.Split(hostname, ":")
	return parts[0]
}

//only save real domain, remove protocol part
func (this *PoliteVisitor) canonicalDomain(domain string) string {
	parts := strings.Split(domain, "//")
	if len(parts) > 1 {
		return parts[1]
	}
	return parts[0]
}

func (this *PoliteVisitor) makeRedisKey(host string) string {
	return fmt.Sprintf("vst:%s", host)
}
