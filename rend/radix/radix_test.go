package radix

import (
	"testing"

	"github.com/jackdoe/blackrock/orgrim/spec"
)

func make(n int64) *spec.Context {
	return &spec.Context{CreatedAtNs: n}
}
func TestRadix(t *testing.T) {
	r := NewTree()
	for i := 1; i < 1000000; i++ {
		r.Add(uint64(i), make(int64(i)))
		f := r.Find(uint64(i))
		if f == nil {
			t.Fatalf("%d is nil", i)
		}
		if f.CreatedAtNs != int64(i) {
			t.Fatalf("expected %d got %d", int64(i), f.CreatedAtNs)
		}
	}
	for i := 9223372036854775807; i > 9223372036854775000; i-- {
		r.Add(uint64(i), make(int64(i)))
		f := r.Find(uint64(i))
		if f == nil {
			t.Fatalf("%d is nil", i)
		}
		if f.CreatedAtNs != int64(i) {
			t.Fatalf("expected %d got %d", int64(i), f.CreatedAtNs)
		}
	}

	r.Add(uint64(0xff0000), make(0xff0000))
	r.Add(uint64(0xffff00), make(0xffff00))

	if r.Find(0xfff000).CreatedAtNs != 0xffff00 {
		t.Fatalf("wrong, got %x", r.Find(0xffff00).CreatedAtNs)
	}

	if r.Find(0xff00ff).CreatedAtNs != 0xff0000 {
		t.Fatalf("wrong, got %x", r.Find(0xff0000).CreatedAtNs)
	}

}

func BenchmarkRadixAdd(b *testing.B) {
	r := NewTree()
	v := &spec.Context{CreatedAtNs: 1}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		r.Add(1, v)
	}
}

func BenchmarkRadixFind(b *testing.B) {
	r := NewTree()
	v := &spec.Context{CreatedAtNs: 1}
	for n := 0; n < b.N; n++ {
		r.Add(uint64(1), v)
	}
	sum := int64(0)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		sum += r.Find(1).CreatedAtNs
	}
}

func BenchmarkMapSet(b *testing.B) {
	r := map[uint64]*spec.Context{}
	v := &spec.Context{CreatedAtNs: 1}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r[uint64(1123123)] = v
	}
}

func BenchmarkMapFind(b *testing.B) {
	r := map[uint64]*spec.Context{}
	v := &spec.Context{CreatedAtNs: 1}
	for n := 0; n < b.N; n++ {
		r[1123123] = v
	}
	sum := int64(0)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		sum += r[1123123].CreatedAtNs
	}
}
