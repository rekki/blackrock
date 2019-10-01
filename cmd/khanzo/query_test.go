package main

import (
	"testing"
)

func postingsListToInts(x []uint64) []int32 {
	list := make([]int32, len(x))
	for i, v := range x {
		list[i] = int32((uint64(v)) >> 32)
	}
	return list

}
func postingsList(n int) []uint64 {
	list := make([]uint64, n)
	for i := 0; i < n; i++ {
		list[i] = (uint64(i) * 3) << 32
	}
	return list
}

func postingsListFromInts(x ...int) []uint64 {
	list := make([]uint64, len(x))
	for i, v := range x {
		list[i] = (uint64(v)) << 32
	}
	return list
}

func query(query Query) []int32 {
	out := []int32{}
	for query.Next() != NO_MORE {
		out = append(out, query.GetDocId())
	}
	return out
}

func eq(t *testing.T, a []int32, b []int32) {
	if len(a) != len(b) {
		t.Logf("len(a) != len(b) ; len(a) = %d, len(b) = %d [%v %v]", len(a), len(b), a, b)
		t.FailNow()
	}

	for i, _ := range a {
		if a[i] != b[i] {
			t.Log("a[i] != b[i]")
			t.FailNow()
		}
	}
}

func BenchmarkNext1000(b *testing.B) {
	x := postingsList(1000)

	for n := 0; n < b.N; n++ {
		sum := int32(0)
		q := NewTerm("", x)
		for q.Next() != NO_MORE {
			sum += q.GetDocId()
		}
	}
}

func BenchmarkOr1000(b *testing.B) {
	x := postingsList(1000)
	y := postingsList(1000)

	for n := 0; n < b.N; n++ {
		sum := int32(0)
		q := NewBoolOrQuery(
			NewTerm("x", x),
			NewTerm("y", y),
		)

		for q.Next() != NO_MORE {
			sum += q.GetDocId()
		}
	}
}

func BenchmarkAnd1000(b *testing.B) {
	x := postingsList(1000000)
	y := postingsList(1000)

	for n := 0; n < b.N; n++ {
		sum := int32(0)
		q := NewBoolAndQuery(
			NewTerm("x", x),
			NewTerm("y", y),
		)

		for q.Next() != NO_MORE {
			sum += q.GetDocId()
		}
	}
}
func dt(docId, t int32) uint64 {
	return uint64(docId)<<32 | uint64(t)
}
func TestAndThen(t *testing.T) {
	click := []uint64{dt(1, 100), dt(2, 100), dt(3, 250)}
	buy := []uint64{dt(30, 10), dt(40, 101), dt(41, 102), dt(42, 120), dt(50, 251), dt(100, 300)}
	res := query(NewAndThenQuery(
		NewTerm("click", click),
		NewTerm("buy", buy),
		2,
	))
	eq(t, []int32{40, 41, 50}, res)
}

func TestModify(t *testing.T) {
	a := postingsList(100)
	b := postingsList(1000)
	c := postingsList(10000)
	d := postingsList(100000)
	e := postingsList(1000000)

	eq(t, postingsListToInts(a), query(NewTerm("x", a)))
	eq(t, postingsListToInts(b), query(NewTerm("x", b)))
	eq(t, postingsListToInts(c), query(NewTerm("x", c)))
	eq(t, postingsListToInts(d), query(NewTerm("x", d)))
	eq(t, postingsListToInts(e), query(NewTerm("x", e)))

	eq(t, postingsListToInts(b), query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTerm("x", b),
	)))

	eq(t, postingsListToInts(c), query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTerm("x", c),
	)))

	eq(t, postingsListToInts(c), query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTerm("x", c),
	)))

	eq(t, postingsListToInts(e), query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTerm("x", d),
		NewTerm("x", e),
	)))

	eq(t, postingsListToInts(a), query(NewBoolAndQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTerm("x", c),
		NewTerm("x", d),
		NewTerm("x", e),
	)))

	eq(t, postingsListToInts(a), query(NewBoolAndQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTerm("x", c),
		NewTerm("x", d),
		NewTerm("x", e),
	)))

	eq(t, []int32{4, 6, 7, 8, 10}, query(NewBoolAndNotQuery(
		NewTerm("x", postingsListFromInts(1, 2, 3, 9)),
		NewBoolOrQuery(
			NewTerm("x", postingsListFromInts(3, 4)),
			NewTerm("x", postingsListFromInts(1, 2, 3, 6, 7, 8, 9, 10)),
		),
	)))
	eq(t, []int32{6, 7, 8, 10}, query(NewBoolAndNotQuery(
		NewTerm("x", postingsListFromInts(1, 2, 3, 9)),
		NewBoolAndNotQuery(
			NewTerm("x", postingsListFromInts(4, 5)),
			NewTerm("x", postingsListFromInts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
			NewTerm("x", postingsListFromInts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
		),
	)))

	eq(t, []int32{6, 7, 8, 10}, query(NewBoolAndNotQuery(
		NewBoolOrQuery(
			NewTerm("x", postingsListFromInts(1, 2)),
			NewTerm("x", postingsListFromInts(3, 9))),
		NewBoolAndNotQuery(
			NewTerm("x", postingsListFromInts(4, 5)),
			NewTerm("x", postingsListFromInts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
			NewTerm("x", postingsListFromInts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
		),
	)))

	eq(t, []int32{}, query(NewBoolAndNotQuery(
		NewTerm("x", postingsListFromInts(1, 2, 3, 9)),
		NewTerm("x", postingsListFromInts(1, 2, 3, 9)),
	)))

	eq(t, []int32{}, query(NewBoolAndNotQuery(
		NewTerm("x", postingsListFromInts(1, 2, 3, 9)),
	)))

	eq(t, []int32{1, 2, 3, 9}, query(NewBoolAndNotQuery(
		NewTerm("x", []uint64{}),
		NewTerm("x", postingsListFromInts(1, 2, 3, 9)),
	)))

	eq(t, postingsListToInts(b), query(NewBoolAndQuery(
		NewBoolOrQuery(
			NewTerm("x", a),
			NewTerm("x", b),
		),
		NewTerm("x", b),
		NewTerm("x", c),
		NewTerm("x", d),
		NewTerm("x", e),
	)))
}
