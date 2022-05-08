package filter

import (
	"testing"

	"go2ch/go2ch/config"

	"github.com/stretchr/testify/assert"
)

func TestDropFilter(t *testing.T) {
	tests := []struct {
		name       string
		input      map[string]interface{}
		conditions []config.Condition
		expect     map[string]interface{}
	}{
		{
			name: "match",
			input: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
			conditions: []config.Condition{
				{
					Key:   "a",
					Value: "aa",
					Type:  typeMatch,
					Op:    opAnd,
				},
			},
			expect: nil,
		},
		{
			name: "contains",
			input: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
			conditions: []config.Condition{
				{
					Key:   "a",
					Value: "a",
					Type:  typeContains,
					Op:    opAnd,
				},
			},
			expect: nil,
		},
		{
			name: "and drop",
			input: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
			conditions: []config.Condition{
				{
					Key:   "a",
					Value: "aa",
					Type:  typeMatch,
					Op:    opAnd,
				},
				{
					Key:   "b",
					Value: "b",
					Type:  typeContains,
					Op:    opAnd,
				},
			},
			expect: nil,
		},
		{
			name: "and not drop",
			input: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
			conditions: []config.Condition{
				{
					Key:   "a",
					Value: "aa",
					Type:  typeMatch,
					Op:    opAnd,
				},
				{
					Key:   "b",
					Value: "c",
					Type:  typeContains,
					Op:    opAnd,
				},
			},
			expect: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
		},
		{
			name: "or drop",
			input: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
			conditions: []config.Condition{
				{
					Key:   "a",
					Value: "aa",
					Type:  typeMatch,
					Op:    opOr,
				},
				{
					Key:   "b",
					Value: "c",
					Type:  typeContains,
					Op:    opOr,
				},
			},
			expect: nil,
		},
		{
			name: "or not drop",
			input: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
			conditions: []config.Condition{
				{
					Key:   "a",
					Value: "c",
					Type:  typeMatch,
					Op:    opOr,
				},
				{
					Key:   "b",
					Value: "c",
					Type:  typeContains,
					Op:    opOr,
				},
			},
			expect: map[string]interface{}{
				"a": "aa",
				"b": "bb",
				"c": "cc",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := DropFilter(test.conditions)(test.input)
			assert.EqualValues(t, test.expect, actual)
		})
	}
}
