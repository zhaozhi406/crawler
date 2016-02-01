package types

type TaskPack struct {
	TaskId      int32  `json:"task_id"`
	Domain      string `json:"domain"`
	Urlpath     string `json:"urlpath"`
	FollowLinks bool   `json:"follow"`
}
