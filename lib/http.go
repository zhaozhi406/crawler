package lib

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"

	"github.com/qiniu/iconv"
)

type HttpClient struct {
}

func (this *HttpClient) Get(url string) ([]byte, error) {

	resp, err := http.Get(urlStr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	return body, err
}

func (this *HttpClient) Post(url string, params url.Values) ([]byte, error) {
	resp, err := http.PostForm(url, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	return body, err

}

func (this *HttpClient) EncodeQuery(params map[string]string) string {
	v := url.Values{}
	for key, val := range params {
		v.Add(key, val)
	}
	return v.Encode()
}

func (this *HttpClient) IconvHtml(html []byte, destEncoding string) ([]byte, error) {

	strReader := bytes.NewReader(html)
	scanner := bufio.NewScanner(strReader)
	re, err := regexp.Compile("charset=([\\w-]+)")

	srcEncoding := ""
	for scanner.Scan() {
		matches := re.FindStringSubmatch(scanner.Text())
		if matches != nil {
			srcEncoding = matches[1]
			break
		}
	}

	if srcEncoding != "" && srcEncoding != destEncoding {
		codec, err := iconv.Open(destEncoding, srcEncoding)
		defer codec.Close()
		u8str := codec.ConvString(string(html))
		return []byte(u8str), err
	}
	return html, err
}
