package client

import (
	"math/rand"
	"testing"
	"time"
	"unsafe"

	spec "github.com/rekki/blackrock/cmd/orgrim/blackrock_io"
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

func doio(t *testing.T, c *Client, size int) {
	data := &spec.Envelope{
		Metadata: &spec.Metadata{
			Properties:  []spec.KV{spec.KV{Key: "hello", Value: "world"}},
			Search:      []spec.KV{spec.KV{Key: "product", Value: "test"}, spec.KV{Key: "open", Value: RandStringBytesMaskImprSrcUnsafe(size)}},
			ForeignId:   RandStringBytesMaskImprSrcUnsafe(5),
			ForeignType: RandStringBytesMaskImprSrcUnsafe(5),
			EventType:   RandStringBytesMaskImprSrcUnsafe(5),
		},
		Payload: []byte(RandStringBytesMaskImprSrcUnsafe(size)),
	}
	err := c.Push(data)
	if err != nil {
		t.Fatal(err)
	}

	data = &spec.Envelope{
		Metadata: &spec.Metadata{
			Properties:  []spec.KV{spec.KV{Key: "hello", Value: "world"}},
			Search:      []spec.KV{spec.KV{Key: "product", Value: "test_wrong"}, spec.KV{Key: "open", Value: RandStringBytesMaskImprSrcUnsafe(size)}},
			ForeignId:   RandStringBytesMaskImprSrcUnsafe(5),
			ForeignType: RandStringBytesMaskImprSrcUnsafe(5),
			EventType:   RandStringBytesMaskImprSrcUnsafe(5),
		},
		Payload: []byte(RandStringBytesMaskImprSrcUnsafe(size)),
	}
	err = c.Push(data)
	if err == nil {
		t.Fatal("expected error")
	}

}

// TODO(aymeric): launch an orgrim instance
// func TestExample(t *testing.T) {
// 	c := NewClient("http://localhost:9001/", "test_token", nil)
// 	for i := 0; i < 1000; i++ {
// 		doio(t, c, i)
// 	}
// }
