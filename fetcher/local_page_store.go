package fetcher

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/kdar/factorlog"
)

type LocalPageStore struct {
	dir string
}

func InitLocalPageStore(dir string) *LocalPageStore {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Errorln("LocalPageStore mkdir ", dir, " error: ", err)
		return nil
	}
	return &LocalPageStore{dir: dir}
}

func (this *LocalPageStore) Save(domain string, urlpath string, page string) error {
	md5Bytes := md5.Sum([]byte(urlpath))
	domain = this.canonicalDomain(domain)
	destDir := filepath.Join(this.dir, domain, fmt.Sprintf("%x", md5Bytes))
	log.Debugln("destDir: ", destDir)
	err := os.MkdirAll(destDir, os.ModePerm)
	if err != nil {
		log.Errorln("mkdir for '"+domain+"/"+urlpath+"' error: ", err)
	} else {
		now := time.Now().Unix()
		fname := fmt.Sprintf("%s/%d", destDir, now)
		err = ioutil.WriteFile(fname, []byte(page), 0666)
		if err != nil {
			log.Errorln("save page: "+domain+"/"+urlpath+" error:", err)
		}
	}

	return err
}

//only save real domain, remove protocol part
func (this *LocalPageStore) canonicalDomain(domain string) string {
	parts := strings.Split(domain, "//")
	if len(parts) > 1 {
		return parts[1]
	}
	return parts[0]
}
