package filter

import (
	"strings"

	"go2ch/go2ch/config"
)

// DropFilter is used to drop data according to user-specified conditions
// The data that meets this condition will be removed when processing and will not be entered into clickhouse
// According to delete condition, specify the value of the key field and Value, the Type field can be (contains) or (match)
func DropFilter(conditions []config.Condition) FilterFunc {
	return func(m map[string]interface{}) map[string]interface{} {
		var qualify bool
		for _, condition := range conditions {
			var qualifyOnce bool
			switch condition.Type {
			case typeMatch:
				qualifyOnce = condition.Value == m[condition.Key]
			case typeContains:
				if val, ok := m[condition.Key].(string); ok {
					qualifyOnce = strings.Contains(val, condition.Value)
				}
			}

			switch condition.Op {
			case opAnd:
				// and: not match once, return original
				if !qualifyOnce {
					return m
				} else {
					// and: match once, pass this, check next
					qualify = true
				}
			case opOr:
				// or: match once, drop
				if qualifyOnce {
					qualify = true
				}
				// or: not match, check next
			}
		}

		if qualify {
			return nil
		} else {
			return m
		}
	}
}
