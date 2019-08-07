package disk

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/jackdoe/blackrock/depths"
)

type InvertedCase struct {
	key   uint64
	value uint64
	data  []int64
}

func Equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
func TestInverted(t *testing.T) {
	dir, err := ioutil.TempDir("", "inverted")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	inv, err := NewInvertedWriter(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	cases := []InvertedCase{InvertedCase{
		key:   0,
		value: 0,
		data:  []int64{1, 2, 3},
	},
		InvertedCase{
			key:   1,
			value: 1,
			data:  []int64{6, 7, 9},
		},
		InvertedCase{
			key:   0,
			value: 1,
			data:  []int64{6, 7, 9},
		},
		InvertedCase{
			key:   1,
			value: 0,
			data:  []int64{6, 7, 9},
		},
	}

	segmentId := depths.SegmentFromNs(time.Now().UnixNano())
	for _, v := range cases {
		for _, id := range v.data {
			err := inv.Append(segmentId, id, v.key, fmt.Sprintf("%d", v.value))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, v := range cases {
		data := inv.Read(segmentId, v.key, fmt.Sprintf("%d", v.value))

		if !Equal(data, v.data) {
			t.Fatalf("mismatch got %v expected %v", data, v.data)
		}
	}

	segment, err := ReadAllTermsInField(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(segment) != 2 {
		t.Fatalf("expected 2 got %d", len(segment))
	}
}
