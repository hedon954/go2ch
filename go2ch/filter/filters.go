package filter

import (
	"go2ch/go2ch/config"
)

const (
	filterDrop         = "drop"
	filterRemoveFields = "remove_field"
	filterTransfer     = "transfer"
	filterTimeFormat   = "time_format"
	opAnd              = "and"
	opOr               = "or"
	typeContains       = "contains"
	typeMatch          = "match"
)

type FilterFunc func(map[string]interface{}) map[string]interface{}

// CreateFilters creates a serial of filters according to cluster config.
func CreateFilters(p *config.Cluster) []FilterFunc {
	var filters []FilterFunc

	for _, f := range p.Filters {
		switch f.Action {
		case filterDrop:
			filters = append(filters, DropFilter(f.Conditions))
		case filterRemoveFields:
			filters = append(filters, RemoveFieldFilter(f.Fields))
		case filterTransfer:
			filters = append(filters, TransferFilter(f.Field, f.Target))
		case filterTimeFormat:
			filters = append(filters, TimeFormatFilter(f.Field, f.Layout, f.Local))
		}
	}

	return filters
}
