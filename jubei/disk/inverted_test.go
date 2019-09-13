package disk

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/jackdoe/blackrock/depths"
)

type InvertedCase struct {
	key   uint32
	value uint32
	data  []int32
}

func Equal(a, b []int32) bool {
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
	inv, err := NewInvertedWriter(10)
	if err != nil {
		t.Fatal(err)
	}
	cases := []InvertedCase{InvertedCase{
		key:   0,
		value: 0,
		data:  []int32{1, 2, 3},
	},
		InvertedCase{
			key:   1,
			value: 1,
			data:  []int32{6, 7, 9},
		},
		InvertedCase{
			key:   0,
			value: 1,
			data:  []int32{6, 7, 9},
		},
		InvertedCase{
			key:   1,
			value: 0,
			data:  []int32{6, 7, 9},
		},
	}

	segmentId := path.Join(dir, depths.SegmentFromNs(time.Now().UnixNano()))
	for _, v := range cases {
		for _, id := range v.data {
			err := inv.Append(segmentId, id, fmt.Sprintf("%d", v.key), fmt.Sprintf("%d", v.value))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, v := range cases {
		data := InvertedReadRaw(segmentId, -1, fmt.Sprintf("%d", v.key), fmt.Sprintf("%d", v.value))

		if !Equal(data, v.data) {
			t.Fatalf("mismatch got %v expected %v", data, v.data)
		}
	}

	segment, err := ReadAllTermsInSegment(segmentId)
	if err != nil {
		t.Fatal(err)
	}
	err = WriteCompactedIndex(segmentId, segment)
	if err != nil {
		t.Fatal(err)
	}

	if len(segment) != 4 {
		t.Fatalf("expected 4 got %d", len(segment))
	}
}
