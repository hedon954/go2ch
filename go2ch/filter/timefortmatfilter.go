package filter

const (
	Time_Format_Layout_Field = "time_format_layout_10086"
	Time_Format_Local_Field  = "time_format_local_10086"
)

// TimeFormatFilter formats data to time.Time whose type is Date, Datetime or Datetime64
func TimeFormatFilter(field, layout, local string) FilterFunc {
	return func(m map[string]interface{}) map[string]interface{} {
		if _, ok := m[field]; ok {
			m[Time_Format_Layout_Field] = layout
			m[Time_Format_Local_Field] = local
		}
		return m
	}
}
