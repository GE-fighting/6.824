package mr

import "encoding/json"

func Any2String(data interface{}) string {
	marshal, _ := json.Marshal(data)
	return string(marshal)
}
