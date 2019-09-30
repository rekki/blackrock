package main

import (
	"testing"

	"github.com/rekki/blackrock/orgrim/spec"
)

func makeExample(created int64, id string) *spec.Context {
	return &spec.Context{CreatedAtNs: created, ForeignId: id, ForeignType: "0"}
}
func TestInsert(t *testing.T) {
	c := NewContextCache(nil)
	v, _ := c.Lookup("0", "a", 0)
	if v != nil {
		t.Fatalf("expecting nil, got %v", v)
	}

	c.Insert(makeExample(100, "a"))
	v, _ = c.Lookup("0", "a", 0)
	if v != nil {
		t.Fatalf("expecting nil, got %v", v)
	}

	v, _ = c.Lookup("0", "a", 101)
	if v.CreatedAtNs != 100 {
		t.Fatalf("expecting 100, got %d", v.CreatedAtNs)
	}

	c.Insert(makeExample(200, "a"))
	v, _ = c.Lookup("0", "a", 101)
	if v.CreatedAtNs != 100 {
		t.Fatalf("expecting 100, got %d", v.CreatedAtNs)
	}
	v, _ = c.Lookup("0", "a", 200)
	if v.CreatedAtNs != 200 {
		t.Fatalf("expecting 200, got %d", v.CreatedAtNs)
	}

	c.Insert(makeExample(300, "a"))
	v, _ = c.Lookup("0", "a", 305)
	if v.CreatedAtNs != 300 {
		t.Fatalf("expecting 300, got %d", v.CreatedAtNs)
	}
	v, _ = c.Lookup("0", "a", 99)
	if v != nil {
		t.Fatalf("expecting nil, got %v", v)
	}

	c.Insert(makeExample(10, "a"))
	v, _ = c.Lookup("0", "a", 99)
	if v.CreatedAtNs != 10 {
		t.Fatalf("expecting 10, got %d", v.CreatedAtNs)
	}

	c.Insert(makeExample(0, "a"))
	v, _ = c.Lookup("0", "a", 4)
	if v.CreatedAtNs != 0 {
		t.Fatalf("expecting 0, got %d", v.CreatedAtNs)
	}

}
