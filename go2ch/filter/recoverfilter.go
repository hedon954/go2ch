package filter

import (
	"encoding/json"

	"github.com/zeromicro/go-zero/core/logx"
)

// RecoverFilter recovers the value to original value
// the reason to do it:
//			the message would be added Time and Err fields after sent into kafka.
//			so we need to remove them first.
func RecoverFilter(input string) FilterFunc {
	return func(m map[string]interface{}) map[string]interface{} {
		switch input {
		case "kafka":
			n := make(map[string]interface{})
			if text, ok := m["Text"]; ok {
				err := json.Unmarshal([]byte(text.(string)), &n)
				if err != nil {
					logx.Errorf("RecoverFilter | unmarshal m to json failed: %v", err)
					return m
				}
				return n
			} else {
				//logx.Errorf("RecoverFilter | search field Text in m failed")
				return m
			}
		}
		return m
	}
}
