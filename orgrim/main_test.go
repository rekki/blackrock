package main

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/jackdoe/blackrock/orgrim/spec"
)

func Equals(a, b []*spec.KV) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(b, func(x, y int) bool {
		if b[x].Key == b[y].Key {
			return b[x].Value < b[y].Value
		}
		return b[x].Key < b[y].Key
	})
	sort.Slice(a, func(x, y int) bool {
		if a[x].Key == a[y].Key {
			return a[x].Value < a[y].Value
		}
		return a[x].Key < a[y].Key
	})
	for i, v := range a {
		if b[i].Key != v.Key {
			return false
		}
		if b[i].Value != v.Value {
			return false
		}
	}
	return true
}

type TransformCase struct {
	json     string
	expand   bool
	expected []*spec.KV
}

func makeKV(kv ...string) []*spec.KV {
	out := []*spec.KV{}
	for i := 0; i < len(kv); i += 2 {
		out = append(out, &spec.KV{Key: kv[i], Value: kv[i+1]})
	}
	return out
}

func TestTransform(t *testing.T) {
	cases := []TransformCase{
		TransformCase{
			json:     `{"hello":"world"}`,
			expand:   true,
			expected: makeKV("hello", "world"),
		},
		TransformCase{
			json:     `{"hello":{"brave":{"new":"world"}}}`,
			expand:   true,
			expected: makeKV("hello.brave.new", "world"),
		},
		TransformCase{
			json:     `{"hello":{"brave_id":{"17a98329-91f5-4373-9016-0e4e5e65ea4d":true, "17a98329-91f5-4373-aaaa-0e4e5e65ea4d":false}}}`,
			expand:   true,
			expected: makeKV("brave_id", "17a98329-91f5-4373-9016-0e4e5e65ea4d", "hello", "true", "brave_id", "17a98329-91f5-4373-aaaa-0e4e5e65ea4d", "hello", "false"),
		},
		TransformCase{
			json:     `{"hello":{"brave_id":{"17a98329-91f5-4373-9016-0e4e5e65ea4d":true, "17a98329-91f5-4373-aaaa-0e4e5e65ea4d":false}}}`,
			expand:   false,
			expected: makeKV("hello.brave_id.17a98329-91f5-4373-9016-0e4e5e65ea4d", "true", "hello.brave_id.17a98329-91f5-4373-aaaa-0e4e5e65ea4d", "false"),
		},
		TransformCase{
			json:     `{"a":{"b_id":123}}`,
			expand:   true,
			expected: makeKV("a", "123", "b_id", "123"),
		},

		TransformCase{
			json:     `{"restaurant_id":123, "selected":{"restaurant_id":{"a":true,"b":true, "c":false}}}`,
			expand:   true,
			expected: makeKV("restaurant_id", "a", "restaurant_id", "b", "restaurant_id", "c", "restaurant_id", "123", "selected", "true", "selected", "false"),
		},

		TransformCase{
			json:     `{"a":{"b_id":{"c":{"d_id":{"f_id":123, "g_id": "000"}}}}}`,
			expand:   true,
			expected: makeKV("a", "000", "a", "123", "f_id", "123", "g_id", "000", "b_id", "c", "d_id", "000", "d_id", "123"), // this is very pathological case
		},
		TransformCase{
			json:     `{"a":{"b_id":{"c":{"d_id":123}}}}`,
			expand:   true,
			expected: makeKV("b_id", "c", "d_id", "123", "a", "123"),
		},
		TransformCase{
			json:     `{"a":{"b_id":123, "selected":true}}`,
			expand:   true,
			expected: makeKV("b_id", "123", "a.selected", "true", "a", "123"),
		},
		TransformCase{
			json:     `{"a_id": 5, "b_id": 10, "selected": "true"}`,
			expand:   true,
			expected: makeKV("a_id", "5", "b_id", "10", "selected", "true"),
		},
	}

	for _, c := range cases {
		var jv map[string]interface{}
		err := json.Unmarshal([]byte(c.json), &jv)
		if err != nil {
			t.Fatal(err)
		}
		transformed, err := transform(jv, c.expand)
		if err != nil {
			t.Fatal(err)
		}
		if !Equals(c.expected, transformed) {
			t.Fatalf("\nexpected:\n%v\ngot:\n%v\njson:\n%s", dumpObj(c.expected), dumpObj(transformed), dumpObj(jv))
		}
	}

}
