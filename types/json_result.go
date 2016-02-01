package types

type JsonResult struct {
	Err  int32       `json:"err"`
	Msg  string      `json:"msg,omitempty"`
	Data interface{} `json:"data,omitempty"`
}
