package filter

// TransferFilter transfers a filed identifier.
func TransferFilter(field, target string) FilterFunc {
	return func(m map[string]interface{}) map[string]interface{} {
		val, ok := m[field]
		if !ok {
			return m
		}
		if len(target) > 0 {
			m[target] = val
			delete(m, field)
		}

		return m
	}
}
