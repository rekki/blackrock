package index

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	"github.com/rekki/blackrock/pkg/logger"
	"github.com/rekki/go-query/util/go_query_dsl"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
func RandString(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func RandomEnvelope(createdAt int64) *spec.Envelope {
	kv := []spec.KV{}
	for i := 0; i < rand.Intn(10); i++ {
		kv = append(kv, spec.KV{Key: RandString(rand.Intn(10)), Value: RandString(rand.Intn(10))})
	}

	return &spec.Envelope{
		Metadata: &spec.Metadata{
			CreatedAtNs: createdAt,
			EventType:   RandString(10),
			ForeignType: RandString(10),
			ForeignId:   RandString(10),
			Search:      kv,
		},
	}
}

func RandomEnvelopes(n int, createdAt int64) []*spec.Envelope {
	out := make([]*spec.Envelope, n)
	for i := 0; i < n; i++ {
		out[i] = RandomEnvelope(createdAt)
	}
	return out
}

func init() {
	logger.LogInit(3)
}

func TestSearchCache(t *testing.T) {
	for _, doCache := range []bool{true, false} {
		root, err := ioutil.TempDir("", "si")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(root)
		si := NewSearchIndex(root, 0, 3600, doCache, map[string]bool{})
		query := &spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}

		inserted := uint64(1000)
		for i := 0; i < int(inserted); i++ {
			err = si.Ingest(RandomEnvelope(1))
			if err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 10; i++ {
			matching := uint64(0)
			err = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
				m := &spec.Metadata{}
				err := s.ReadForwardDecode(did, m)
				if err != nil {
					t.Fatal(err)
				}
				if m.CreatedAtNs != 1 {
					t.Fatal(err)
				}
				matching++
				return nil
			})

			if err != nil {
				t.Fatal(err)
			}

			if matching != inserted {
				t.Fatalf("expected %d got %d", inserted, matching)
			}
		}
		si.Close()
	}
}

func TestSearchIndexBasic(t *testing.T) {
	root, err := ioutil.TempDir("", "si")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	si := NewSearchIndex(root, 0, 3600, false, map[string]bool{})
	si.PrintStats()
	err = si.Ingest(&spec.Envelope{Metadata: nil})
	if err != errMissingMetadata {
		t.Fatal("expected missing metadata")
	}

	err = si.Ingest(&spec.Envelope{Metadata: &spec.Metadata{Search: []spec.KV{spec.KV{Key: "xx", Value: "yy"}}}})
	if err != errMissingForeignId {
		t.Fatal("expected missing foreignId")
	}

	err = si.Ingest(&spec.Envelope{Metadata: &spec.Metadata{ForeignId: "aa", Search: []spec.KV{spec.KV{Key: "xx", Value: "yy"}}}})
	if err != errMissingForeignType {
		t.Fatal("expected missing foreignType")
	}

	err = si.Ingest(&spec.Envelope{Metadata: &spec.Metadata{ForeignType: "bb", ForeignId: "aa", Search: []spec.KV{spec.KV{Key: "xx", Value: "yy"}}}})
	if err != errMissingEventType {
		t.Fatal("expected missing eventType")
	}

	query := &spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}

	matching := uint64(0)
	err = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
		matching++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if matching != 0 {
		t.Fatal("expected 0")
	}

	inserted := uint64(1000)
	for i := 0; i < int(inserted); i++ {
		err = si.Ingest(RandomEnvelope(1))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = si.ForEach(&spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: nil}, 0, func(s *Segment, did int32, score float32) error {
		return nil
	})
	if err != errBadRequest {
		t.Fatal("expected errBadRequest")
	}

	err = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
		matching++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if matching != inserted {
		t.Fatalf("expected %d got %d", inserted, matching)
	}

	err = si.DumpToDisk()
	if err != nil {
		t.Fatal(err)
	}
	si.PrintStats()
	si.Close()

	matching = uint64(0)
	si = NewSearchIndex(root, 0, 3600, true, map[string]bool{})
	err = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
		atomic.AddUint64(&matching, 1)
		b, err := s.ReadForward(did)
		if err != nil {
			t.Fatal(err)
		}

		m := &spec.Metadata{}
		err = proto.Unmarshal(b, m)
		if err != nil {
			t.Fatal(err)
		}

		if m.CreatedAtNs != 1 {
			t.Fatal("expected 1")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = si.ForEach(&spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "not_existing"}}, 0, func(s *Segment, did int32, score float32) error {
		t.Fatal("should not exist")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = si.ForEach(&spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "not_existing", Value: "not_existing"}}, 0, func(s *Segment, did int32, score float32) error {
		t.Fatal("should not exist")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// nothing should be found now
	err = si.ForEach(&spec.SearchQueryRequest{FromSecond: 0, ToSecond: 0, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}, 0, func(s *Segment, did int32, score float32) error {
		t.Fatal("should not exist")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if matching != inserted {
		t.Fatalf("expected %d got %d", inserted, matching)
	}

	shouldStop := 0
	expectedError := errors.New("NOOOOOO")
	err = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
		shouldStop++
		return expectedError
	})
	if err != expectedError {
		t.Fatal("unexpected error")
	}
	if shouldStop != 1 {
		t.Fatal("expected 1")
	}

	limit := 10
	count := 0
	err = si.ForEach(query, uint32(limit), func(s *Segment, did int32, score float32) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if limit != count {
		t.Fatal("expected limit")
	}

	si.Close()
}

func TestConcurrentReadAndWrite(t *testing.T) {
	root, err := ioutil.TempDir("", "si")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	si := NewSearchIndex(root, 0, 3600, true, map[string]bool{})
	writers := 15
	readers := 15
	written := uint64(0)
	read := uint64(0)
	readWhileWrite := uint64(0)
	query := &spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}
	wg := sync.WaitGroup{}

	for i := 0; i < 10000; i++ {
		err = si.Ingest(RandomEnvelope(1))
		if err != nil {
			panic(err)
		}
		atomic.AddUint64(&written, 1)
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				hour := rand.Int() % 2

				err = si.Ingest(RandomEnvelope(1 + (int64(hour) * 3600 * 1e9)))
				if err != nil {
					panic(err)
				}
				atomic.AddUint64(&written, 1)
			}
			wg.Done()
		}()
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				time.Sleep(10 * time.Millisecond)

				err := si.DumpToDisk()
				if err != nil {
					panic(err)
				}

			}

			wg.Done()
		}()
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 10000; i++ {
				old := 0
				err = si.ForEach(query, 10, func(s *Segment, did int32, score float32) error {
					if old != s.TotalDocs {
						atomic.AddUint64(&readWhileWrite, 1)
					}
					m := &spec.Metadata{}

					err := s.ReadForwardDecode(did, m)
					if err != nil {
						panic(err)
					}

					if m.CreatedAtNs == 0 {
						panic("expected non 0")
					}
					atomic.AddUint64(&read, 1)
					old = s.TotalDocs
					return nil
				})
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if written == 0 || read == 0 || readWhileWrite == 0 {
		t.Fatal("expected something to happen")
	}

	found := uint64(0)
	err = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
		m := &spec.Metadata{}
		err := s.ReadForwardDecode(did, m)
		if err != nil {
			panic(err)
		}

		if m.CreatedAtNs == 0 {
			panic("expected non zero")
		}
		found++
		return nil

	})
	if found != written {
		t.Fatalf("found(%d) != written(%d)", found, written)
	}

	si.Close()
}

var dontOptimizeMe = 0

func BenchmarkIngest1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		root, err := ioutil.TempDir("", "si")
		if err != nil {
			panic(err)
		}

		si := NewSearchIndex(root, 0, 3600, false, map[string]bool{})
		n := 1000
		envelopes := RandomEnvelopes(n, 1)
		b.StartTimer()

		for _, v := range envelopes {
			err = si.Ingest(v)
			if err != nil {
				panic(err)
			}

			dontOptimizeMe++
		}
		b.StopTimer()
		os.RemoveAll(root)
	}
}

func BenchmarkSearch1000000(b *testing.B) {
	b.StopTimer()
	root, err := ioutil.TempDir("", "si")
	if err != nil {
		panic(err)
	}

	si := NewSearchIndex(root, 0, 3600, false, map[string]bool{})
	n := 1000000
	for i := 0; i < n; i++ {
		err = si.Ingest(RandomEnvelope(1))
		if err != nil {
			panic(err)
		}
	}

	query := &spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		matching := 0
		_ = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
			matching++
			dontOptimizeMe++
			return nil
		})

		if matching != n {
			panic("no")
		}
	}
	b.StopTimer()
	os.RemoveAll(root)
}

func BenchmarkSearchDecode1000000(b *testing.B) {
	b.StopTimer()
	root, err := ioutil.TempDir("", "si")
	if err != nil {
		panic(err)
	}

	si := NewSearchIndex(root, 0, 3600, false, map[string]bool{})
	n := 1000000
	for i := 0; i < n; i++ {
		err = si.Ingest(RandomEnvelope(1))
		if err != nil {
			panic(err)
		}
	}

	query := &spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		matching := 0
		m := spec.BasicMetadata{}
		_ = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
			matching++
			err := s.ReadForwardDecode(did, &m)
			if err != nil {
				panic(err)
			}
			if m.CreatedAtNs != 1 {
				panic("1")
			}
			dontOptimizeMe++
			return nil
		})

		if matching != n {
			panic("no")
		}
	}
	b.StopTimer()
	os.RemoveAll(root)

}

func BenchmarkSearchRead1000000(b *testing.B) {
	b.StopTimer()
	root, err := ioutil.TempDir("", "si")
	if err != nil {
		panic(err)
	}

	si := NewSearchIndex(root, 0, 3600, false, map[string]bool{})
	n := 1000000
	for i := 0; i < n; i++ {
		err = si.Ingest(RandomEnvelope(1))
		if err != nil {
			panic(err)
		}
	}

	query := &spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		matching := 0
		_ = si.ForEach(query, 0, func(s *Segment, did int32, score float32) error {
			matching++
			b, err := s.ReadForward(did)
			if err != nil {
				panic(err)
			}
			dontOptimizeMe += len(b)
			return nil
		})

		if matching != n {
			panic("no")
		}
	}
	b.StopTimer()
	os.RemoveAll(root)

}

func BenchmarkSearchRead10(b *testing.B) {
	b.StopTimer()
	root, err := ioutil.TempDir("", "si")
	if err != nil {
		panic(err)
	}

	si := NewSearchIndex(root, 0, 3600, true, map[string]bool{})
	n := 1000000
	for i := 0; i < n; i++ {
		err = si.Ingest(RandomEnvelope(1))
		if err != nil {
			panic(err)
		}
	}

	query := &spec.SearchQueryRequest{FromSecond: 1, ToSecond: 7200, Query: &go_query_dsl.Query{Field: "blackrock", Value: "match_all"}}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		matching := 0
		m := spec.BasicMetadata{}
		_ = si.ForEach(query, 10, func(s *Segment, did int32, score float32) error {
			matching++
			err := s.ReadForwardDecode(did, &m)
			if err != nil {
				panic(err)
			}

			dontOptimizeMe += int(m.CreatedAtNs)
			return nil
		})

		if matching != 10 {
			panic("no")
		}
	}
	b.StopTimer()
	os.RemoveAll(root)

}

func BenchmarkStore1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		root, err := ioutil.TempDir("", "si")
		if err != nil {
			panic(err)
		}

		si := NewSearchIndex(root, 0, 3600, true, map[string]bool{})
		n := 1000
		envelopes := RandomEnvelopes(n, 1)

		for _, v := range envelopes {
			err = si.Ingest(v)
			if err != nil {
				panic(err)
			}

			dontOptimizeMe++
		}
		b.StartTimer()
		err = si.DumpToDisk()
		if err != nil {
			panic(err)
		}

		b.StopTimer()
		os.RemoveAll(root)
	}
}
