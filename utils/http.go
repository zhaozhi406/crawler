package utils

import (
	"net/http"
	"strconv"
	"github.com/zhaozhi406/crawler/types"
)

//检查http请求中必须的参数是否存在，空字符串也算不存在；
//给出类型的，还要验证类型是否合法；
//检查到不存在的key，或参数类型错误立即返回该key
func CheckHttpParams(req *http.Request, keys map[string]string) (string, error) {
	req.ParseForm()

	for key, t := range keys {
		val := req.Form.Get(key)
		if val == '' {
			return key, errors.New("missing http param: " + key)
		}
		if t == "int"  {
			_, err := strconv.Atoi(val)
			if err != nil {
				return key, errors.New("http param '" + key + "' type error, require " + t)
			}
		}

	}
	return '', nil
}

//输出JsonResult到http response
func OutputJsonResult(w http.ResponseWriter, result types.JsonResult) {
	resultJson, err := json.Marshal(result)
	if err != nil {
		log.Debugln("encode json result error: ", err)
	}
	w.Write(resultJson)
}
