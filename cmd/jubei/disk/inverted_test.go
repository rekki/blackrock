package disk

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/rekki/blackrock/pkg/depths"
)

type InvertedCase struct {
	key   uint32
	value uint32
	data  []uint64
}

func Equal(a, b []uint64) bool {
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

func makePostingsList(a ...int) []uint64 {
	out := make([]uint64, len(a))
	for i, v := range a {
		out[i] = uint64(v)<<32 | 1
	}
	return out
}
func TestInverted(t *testing.T) {
	dir, err := ioutil.TempDir("", "inverted")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	segmentId := path.Join(dir, depths.SegmentFromNs(time.Now().UnixNano()))
	inv := NewInvertedWriter(segmentId)
	cases := []InvertedCase{InvertedCase{
		key:   0,
		value: 0,
		data:  makePostingsList(1, 2, 3),
	},
		InvertedCase{
			key:   1,
			value: 1,
			data:  makePostingsList(6, 7, 9),
		},
		InvertedCase{
			key:   0,
			value: 1,
			data:  makePostingsList(6, 7, 9),
		},
		InvertedCase{
			key:   1,
			value: 0,
			data:  makePostingsList(6, 7, 9),
		},
	}

	for _, v := range cases {
		for _, id := range v.data {
			inv.Append(int32(id>>32), 1, fmt.Sprintf("%d", v.key), fmt.Sprintf("%d", v.value))
		}
	}
	err = inv.Flush()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range cases {
		data := InvertedReadRaw(segmentId, -1, fmt.Sprintf("%d", v.key), fmt.Sprintf("%d", v.value))

		if !Equal(data, v.data) {
			t.Fatalf("mismatch got %v expected %v", data, v.data)
		}
	}
}
