package main

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"
)

func postingsList(n int) []int32 {
	list := make([]int32, n)
	for i := 0; i < n; i++ {
		list[i] = int32(i) * 3
	}
	return list
}

func postingsListFile(root string, n int) *os.File {
	data := make([]byte, n*8)
	for i := 0; i < n; i++ {
		v := int32(i) * 3
		binary.LittleEndian.PutUint32(data[i*8:], uint32(v))
	}

	f, err := ioutil.TempFile(root, "postings_")
	if err != nil {
		panic(err)
	}
	_, err = f.Write(data)
	if err != nil {
		panic(err)
	}
	return f
}

func query(query Query) []int32 {
	out := []int32{}
	for query.Next() != NO_MORE {
		out = append(out, query.GetDocId())
	}
	return out
}

func eq(t *testing.T, a, b []int32) {
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

func BenchmarkNextFile1000(b *testing.B) {
	tmp, err := ioutil.TempDir("", "query_test")
	if err != nil {
		panic(err)
	}

	x := postingsListFile(tmp, 1000)
	defer x.Close()
	defer os.RemoveAll(tmp)

	for n := 0; n < b.N; n++ {
		sum := int32(0)
		q := NewTermFile("", x)
		for q.Next() != NO_MORE {
			sum += q.GetDocId()
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

func TestModify(t *testing.T) {
	a := postingsList(100)
	b := postingsList(1000)
	c := postingsList(10000)
	d := postingsList(100000)
	e := postingsList(1000000)
	tmp, err := ioutil.TempDir("", "query_test")
	if err != nil {
		t.Fatal(err)
	}
	af := postingsListFile(tmp, 100)
	bf := postingsListFile(tmp, 1000)
	cf := postingsListFile(tmp, 10000)
	df := postingsListFile(tmp, 100000)
	ef := postingsListFile(tmp, 1000000)
	defer af.Close()
	defer bf.Close()
	defer cf.Close()
	defer df.Close()
	defer ef.Close()
	defer os.RemoveAll(tmp)

	eq(t, a, query(NewTerm("x", a)))
	eq(t, b, query(NewTerm("x", b)))
	eq(t, c, query(NewTerm("x", c)))
	eq(t, d, query(NewTerm("x", d)))
	eq(t, e, query(NewTerm("x", e)))

	eq(t, a, query(NewTermFile("x", af)))
	eq(t, b, query(NewTermFile("x", bf)))
	eq(t, c, query(NewTermFile("x", cf)))
	eq(t, d, query(NewTermFile("x", df)))
	eq(t, e, query(NewTermFile("x", ef)))

	eq(t, b, query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTerm("x", b),
	)))

	eq(t, b, query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTermFile("x", bf),
	)))

	eq(t, c, query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTerm("x", c),
	)))

	eq(t, c, query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTermFile("x", bf),
		NewTerm("x", c),
	)))

	eq(t, e, query(NewBoolOrQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTermFile("x", cf),
		NewTerm("x", d),
		NewTerm("x", e),
	)))

	eq(t, a, query(NewBoolAndQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTerm("x", c),
		NewTerm("x", d),
		NewTerm("x", e),
	)))

	eq(t, a, query(NewBoolAndQuery(
		NewTerm("x", a),
		NewTerm("x", b),
		NewTerm("x", c),
		NewTerm("x", d),
		NewTerm("x", e),
	)))

	eq(t, []int32{4, 6, 7, 8, 10}, query(NewBoolAndNotQuery(
		NewTerm("x", []int32{1, 2, 3, 9}),
		NewBoolOrQuery(
			NewTerm("x", []int32{3, 4}),
			NewTerm("x", []int32{1, 2, 3, 6, 7, 8, 9, 10}),
		),
	)))
	eq(t, []int32{6, 7, 8, 10}, query(NewBoolAndNotQuery(
		NewTerm("x", []int32{1, 2, 3, 9}),
		NewBoolAndNotQuery(
			NewTerm("x", []int32{4, 5}),
			NewTerm("x", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			NewTerm("x", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		),
	)))

	eq(t, []int32{6, 7, 8, 10}, query(NewBoolAndNotQuery(
		NewBoolOrQuery(
			NewTerm("x", []int32{1, 2}),
			NewTerm("x", []int32{3, 9})),
		NewBoolAndNotQuery(
			NewTerm("x", []int32{4, 5}),
			NewTerm("x", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			NewTerm("x", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		),
	)))

	eq(t, []int32{}, query(NewBoolAndNotQuery(
		NewTerm("x", []int32{1, 2, 3, 9}),
		NewTerm("x", []int32{1, 2, 3, 9}),
	)))

	eq(t, []int32{}, query(NewBoolAndNotQuery(
		NewTerm("x", []int32{1, 2, 3, 9}),
	)))

	eq(t, []int32{1, 2, 3, 9}, query(NewBoolAndNotQuery(
		NewTerm("x", []int32{}),
		NewTerm("x", []int32{1, 2, 3, 9}),
	)))

	eq(t, b, query(NewBoolAndQuery(
		NewBoolOrQuery(
			NewTerm("x", a),
			NewTerm("x", b),
		),
		NewTerm("x", b),
		NewTerm("x", c),
		NewTerm("x", d),
		NewTerm("x", e),
	)))

	eq(t, c, query(NewBoolAndQuery(
		NewBoolOrQuery(
			NewTermFile("x", af),
			NewTerm("x", b),
			NewBoolAndQuery(
				NewTermFile("x", cf),
				NewTerm("x", d),
			),
		),
		NewTerm("x", d),
		NewTerm("x", e),
	)))
}
