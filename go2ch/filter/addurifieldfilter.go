package filter

import (
	"strings"

	"github.com/globalsign/mgo/bson"
)

// AddUriFieldFilter is used to add uri filed to m according to other specified inFiled
func AddUriFieldFilter(inField, outField string) FilterFunc {
	return func(m map[string]interface{}) map[string]interface{} {
		if val, ok := m[inField].(string); ok {
			var datas []string
			idx := strings.Index(val, "?")
			if idx < 0 {
				datas = strings.Split(val, "/")
			} else {
				datas = strings.Split(val[:idx], "/")
			}

			for i, data := range datas {
				if bson.IsObjectIdHex(data) {
					datas[i] = "*"
				}
			}

			m[outField] = strings.Join(datas, "/")
		}
		return m
	}
}