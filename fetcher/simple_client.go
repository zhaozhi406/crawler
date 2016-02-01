package fetcher

import (
	"bufio"
	"bytes"
	"github.com/qiniu/iconv"
	"http"
	"net/http"
	"regexp"
)

type SimpleClient struct {
}

func (this *SimpleClient) Get(urlStr string) (string, error) {
	resp, err := http.Get(urlStr)
	if err != nil {
		log.Println(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	log.Println(err)

	strReader := bytes.NewReader(body)
	scanner := bufio.NewScanner(strReader)
	re, _ := regexp.Compile("charset=([\\w-]+)")

	srcEncoding := ""
	for scanner.Scan() {
		matches := re.FindStringSubmatch(scanner.Text())
		if matches != nil {
			log.Println(matches)
			srcEncoding = matches[1]
			break
		}
	}

	if srcEncoding != "" {
		codec, err := iconv.Open("utf-8", srcEncoding)
		defer codec.Close()
		u8str := codec.ConvString(string(body))
		return u8str, err
	}
	return string(body), err
}
