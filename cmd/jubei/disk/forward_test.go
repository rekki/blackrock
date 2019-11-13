package disk

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var src = rand.NewSource(time.Now().UnixNano())

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

type Case struct {
	id       uint32
	document uint32
	data     []byte
}

func TestFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fw, err := NewForwardWriter(dir, "forward")
	if err != nil {
		t.Fatal(err)
	}
	cnt := uint64(0)
	cases := []Case{}
	for i := 0; i < 1000; i++ {
		data := []byte(RandStringRunes(i))
		atomic.AddUint64(&cnt, 1)
		off, err := fw.Append(data)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, Case{id: uint32(i), document: off, data: data})
	}
	for _, v := range cases {
		data, _, err := fw.Read(v.document)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v.data, data) {
			t.Fatalf("data mismatch, expected %v got %v", v.data, data)
		}
	}
	fw.Close()

	fw, err = NewForwardWriter(dir, "forward")
	if err != nil {
		t.Fatal(err)
	}
	s, err := fw.Size()
	if err != nil {
		t.Fatal(err)
	}
	if s == 0 {
		t.Fatal("expected size")
	}
	done := make(chan []Case)
	randomized := [][]byte{}
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("%s-%s", "face", RandStringRunes(i)))
		randomized = append(randomized, data)
	}

	for k := 0; k < 100; k++ {
		go func() {
			out := []Case{}
			for i := 0; i < 1000; i++ {
				data := randomized[i]
				atomic.AddUint64(&cnt, 1)
				off, err := fw.Append(data)
				if err != nil {
					t.Fatal(err)
				}
				out = append(out, Case{id: uint32(i), document: off, data: data})
			}
			done <- out
		}()
	}
	for k := 0; k < 100; k++ {
		x := <-done
		cases = append(cases, x...)
	}
	for _, v := range cases {
		data, _, err := fw.Read(v.document)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(v.data, data) {
			t.Fatalf("data mismatch, expected %v got %v", hex.EncodeToString(v.data), hex.EncodeToString(data))
		}
	}
	n := uint64(0)
	err = fw.Scan(0, func(offset uint32, data []byte) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := cnt
	if n != expected {
		t.Fatalf("expected %d got %d", expected, n)
	}
	fw.Close()
}
