package disk

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
	"unsafe"
)

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImprSrcUnsafe(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

type Case struct {
	id       uint32
	document uint32
	data     []byte
}

func TestForward(t *testing.T) {
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fw, err := NewForwardWriter(dir, "forward")
	if err != nil {
		t.Fatal(err)
	}
	cases := []Case{}
	for i := 0; i < 1000; i++ {
		data := []byte(RandStringBytesMaskImprSrcUnsafe(i))

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
	for i := 0; i < 1000; i++ {
		data := []byte(RandStringBytesMaskImprSrcUnsafe(i))

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
	n := 0
	err = fw.Scan(0, func(offset uint32, data []byte) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := 2000
	if n != expected {
		t.Fatalf("expected %d got %d", expected, n)
	}
	fw.Close()
}
