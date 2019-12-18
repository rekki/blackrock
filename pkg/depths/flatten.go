/*
The MIT License (MIT)

Copyright (c) 2016 Jeremy Wohl

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/
// Flatten makes flat, one-dimensional maps from arbitrarily nested ones.
//
// It turns map keys into compound
// names, in four styles: dotted (`a.b.1.c`), path-like (`a/b/1/c`), Rails (`a[b][1][c]`), or with underscores (`a_b_1_c`).  It takes input as either JSON strings or
// Go structures.  It knows how to traverse these JSON types: objects/maps, arrays and scalars.
//
// You can flatten JSON strings.
//
//	nested := `{
//	  "one": {
//	    "two": [
//	      "2a",
//	      "2b"
//	    ]
//	  },
//	  "side": "value"
//	}`
//
//	flat, err := flatten.FlattenString(nested, "", flatten.DotStyle)
//
//	// output: `{ "one.two.0": "2a", "one.two.1": "2b", "side": "value" }`
//
// Or Go maps directly.
//
//	nested := map[string]interface{}{
//		"a": "b",
//		"c": map[string]interface{}{
//			"d": "e",
//			"f": "g",
//		},
//		"z": 1.4567,
//	}
//
//	flat, err := flatten.Flatten(nested, "", flatten.RailsStyle)
//
//	// output:
//	// map[string]interface{}{
//	//	"a":    "b",
//	//	"c[d]": "e",
//	//	"c[f]": "g",
//	//	"z":    1.4567,
//	// }
//
package depths

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// The presentation style of keys.
type SeparatorStyle int

const (
	_ SeparatorStyle = iota

	// Separate nested key components with dots, e.g. "a.b.1.c.d"
	DotStyle

	// Separate with path-like slashes, e.g. a/b/1/c/d
	PathStyle

	// Separate ala Rails, e.g. "a[b][c][1][d]"
	RailsStyle

	// Separate with underscores, e.g. "a_b_1_c_d"
	UnderscoreStyle
)

// Nested input must be a map or slice
var NotValidInputError = errors.New("Not a valid input: map or slice")

// Flatten generates a flat map from a nested one.  The original may include values of type map, slice and scalar,
// but not struct.  Keys in the flat map will be a compound of descending map keys and slice iterations.
// The presentation of keys is set by style.  A prefix is joined to each key.
func Flatten(nested map[string]interface{}, prefix string, style SeparatorStyle) (map[string]string, error) {
	flatmap := make(map[string]string)

	err := flatten(true, flatmap, nested, prefix, style)
	if err != nil {
		return nil, err
	}

	return flatmap, nil
}

func FlattenArray(nested []interface{}, prefix string, style SeparatorStyle) (map[string]string, error) {
	flatmap := make(map[string]string)

	err := flatten(true, flatmap, nested, prefix, style)
	if err != nil {
		return nil, err
	}

	return flatmap, nil
}

// FlattenString generates a flat JSON map from a nested one.  Keys in the flat map will be a compound of
// descending map keys and slice iterations.  The presentation of keys is set by style.  A prefix is joined
// to each key.
func FlattenString(nestedstr, prefix string, style SeparatorStyle) (string, error) {
	var nested map[string]interface{}
	err := json.Unmarshal([]byte(nestedstr), &nested)
	if err != nil {
		return "", err
	}

	flatmap, err := Flatten(nested, prefix, style)
	if err != nil {
		return "", err
	}

	flatb, err := json.Marshal(&flatmap)
	if err != nil {
		return "", err
	}

	return string(flatb), nil
}

func flatten(top bool, flatMap map[string]string, nested interface{}, prefix string, style SeparatorStyle) error {
	assign := func(newKey string, v interface{}) error {
		switch v := v.(type) {
		case map[string]interface{}, []interface{}:
			if err := flatten(false, flatMap, v, newKey, style); err != nil {
				return err
			}

		case string:
			flatMap[newKey] = v
		case int:
			flatMap[newKey] = fmt.Sprintf("%d", v)
		case int32:
			flatMap[newKey] = fmt.Sprintf("%d", v)
		case int64:
			flatMap[newKey] = fmt.Sprintf("%d", v)
		case int16:
			flatMap[newKey] = fmt.Sprintf("%d", v)
		case uint32:
			flatMap[newKey] = fmt.Sprintf("%d", v)
		case uint64:
			flatMap[newKey] = fmt.Sprintf("%d", v)
		case uint16:
			flatMap[newKey] = fmt.Sprintf("%d", v)
		case float32:
			flatMap[newKey] = strconv.FormatFloat(float64(v), 'f', 0, 64)
		case float64:
			flatMap[newKey] = strconv.FormatFloat(v, 'f', 0, 64)

		default:
			flatMap[newKey] = fmt.Sprintf("%v", v)

		}

		return nil
	}

	switch nested := nested.(type) {
	case map[string]interface{}:
		for k, v := range nested {
			newKey := enkey(top, prefix, k, style)
			_ = assign(newKey, v)
		}
	case []interface{}:
		for i, v := range nested {
			newKey := enkey(top, prefix, strconv.Itoa(i), style)
			_ = assign(newKey, v)
		}
	default:
		return NotValidInputError
	}

	return nil
}

func enkey(top bool, prefix, subkey string, style SeparatorStyle) string {
	key := prefix

	if top {
		key += subkey
	} else {
		switch style {
		case DotStyle:
			key += "." + subkey
		case PathStyle:
			key += "/" + subkey
		case RailsStyle:
			key += "[" + subkey + "]"
		case UnderscoreStyle:
			key += "_" + subkey
		}
	}

	return key
}
