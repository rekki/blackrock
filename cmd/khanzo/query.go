package main

import (
	"fmt"
	"math"
	"strings"
)

const (
	NO_MORE   = int32(math.MaxInt32)
	NOT_READY = int32(-1)
)

type Query interface {
	advance(int32) int32
	Next() int32
	GetDocId() int32
	GetTime() int32
	Score() float32
	Reset()
	String() string
}

type QueryBase struct {
	docId int32
	time  int32
}

func (q *QueryBase) GetDocId() int32 {
	return q.docId
}

func (q *QueryBase) GetTime() int32 {
	return q.time
}

type Term struct {
	cursor   int
	postings []uint64
	term     string
	QueryBase
}

func NewTerm(t string, postings []uint64) *Term {
	return &Term{
		term:      t,
		cursor:    -1,
		postings:  postings,
		QueryBase: QueryBase{NOT_READY, 0},
	}
}

func (t *Term) String() string {
	return t.term
}

func (t *Term) Reset() {
	t.cursor = -1
	t.docId = NOT_READY
	t.time = 0
}

func (t *Term) Score() float32 {
	return float32(1)
}

func (t *Term) advance(target int32) int32 {
	if t.docId == NO_MORE || t.docId == target || target == NO_MORE {
		t.docId = target
		return t.docId
	}
	if t.cursor < 0 {
		t.cursor = 0
	}

	start := t.cursor
	end := len(t.postings)

	for start < end {
		mid := start + ((end - start) / 2)
		current, timeSecond := split(t.postings[mid])
		if current == target {
			t.cursor = mid
			t.docId = target
			t.time = timeSecond
			return target
		}

		if current < target {
			start = mid + 1
		} else {
			end = mid
		}
	}
	if start >= len(t.postings) {
		t.docId = NO_MORE
		t.time = 0
		return NO_MORE
	}
	t.cursor = start
	t.docId, t.time = split(t.postings[start])
	return t.docId
}

func split(x uint64) (int32, int32) {
	return int32(x >> 32), int32(x & 0xffffffff)
}

func (t *Term) Next() int32 {
	t.cursor++
	if t.cursor >= len(t.postings) {
		t.docId = NO_MORE
		t.time = 0
	} else {
		t.docId, t.time = split(t.postings[t.cursor])
	}
	return t.docId
}

type BoolQueryBase struct {
	queries []Query
}

func (q *BoolQueryBase) AddSubQuery(sub Query) {
	q.queries = append(q.queries, sub)
}

type BoolOrQuery struct {
	BoolQueryBase
	QueryBase
}

func NewBoolOrQuery(queries ...Query) *BoolOrQuery {
	return &BoolOrQuery{
		BoolQueryBase: BoolQueryBase{queries},
		QueryBase:     QueryBase{NOT_READY, 0},
	}
}

func (q *BoolOrQuery) Reset() {
	q.docId = NOT_READY
	q.time = 0
	for _, v := range q.queries {
		v.Reset()
	}
}

func (q *BoolOrQuery) Score() float32 {
	score := 0
	n := len(q.queries)
	for i := 0; i < n; i++ {
		sub_query := q.queries[i]
		if sub_query.GetDocId() == q.docId {
			score++
		}
	}
	return float32(score)
}

func (q *BoolOrQuery) advance(target int32) int32 {
	new_doc := NO_MORE
	n := len(q.queries)
	for i := 0; i < n; i++ {
		sub_query := q.queries[i]
		cur_doc := sub_query.GetDocId()
		if cur_doc < target {
			cur_doc = sub_query.advance(target)
		}

		if cur_doc < new_doc {
			new_doc = cur_doc
		}
	}
	q.docId = new_doc
	return q.docId
}

func (q *BoolOrQuery) String() string {
	out := []string{}
	for _, v := range q.queries {
		out = append(out, v.String())
	}
	return strings.Join(out, " OR ")
}

func (q *BoolOrQuery) Next() int32 {
	new_doc := NO_MORE
	n := len(q.queries)
	for i := 0; i < n; i++ {
		sub_query := q.queries[i]
		cur_doc := sub_query.GetDocId()
		if cur_doc == q.docId {
			cur_doc = sub_query.Next()
		}

		if cur_doc < new_doc {
			new_doc = cur_doc
		}
	}
	q.docId = new_doc
	return new_doc
}

type BoolAndQuery struct {
	BoolQueryBase
	QueryBase
	not Query
}

func NewBoolAndNotQuery(not Query, queries ...Query) *BoolAndQuery {
	return NewBoolAndQuery(queries...).SetNot(not)
}

func NewBoolAndQuery(queries ...Query) *BoolAndQuery {
	return &BoolAndQuery{
		BoolQueryBase: BoolQueryBase{queries},
		QueryBase:     QueryBase{NOT_READY, 0},
	}
}

func (q *BoolAndQuery) SetNot(not Query) *BoolAndQuery {
	q.not = not
	return q
}

func (q *BoolAndQuery) Score() float32 {
	return float32(len(q.queries))
}

func (q *BoolAndQuery) nextAndedDoc(target int32) int32 {
	start := 1
	n := len(q.queries)
AGAIN:
	for {
		// initial iteration skips queries[0], because it is used in caller
		for i := start; i < n; i++ {
			sub_query := q.queries[i]
			if sub_query.GetDocId() < target {
				sub_query.advance(target)
			}

			if sub_query.GetDocId() == target {
				continue
			}

			target = q.queries[0].advance(sub_query.GetDocId())

			i = 0 //restart the loop from the first query
		}

		if q.not != nil && q.not.GetDocId() != NO_MORE && target != NO_MORE {
			if q.not.advance(target) == target {
				// the not query is matching, so we have to move on
				// advance everything, set the new target to the highest doc, and start again
				newTarget := target + 1
				for i := 0; i < n; i++ {
					current := q.queries[i].advance(newTarget)
					if current > newTarget {
						newTarget = current
					}
				}
				target = newTarget
				start = 0
				continue AGAIN
			}
		}

		q.docId = target
		return q.docId
	}
}
func (q *BoolAndQuery) Reset() {
	q.docId = NOT_READY
	q.time = 0
	for _, v := range q.queries {
		v.Reset()
	}
}

func (q *BoolAndQuery) String() string {
	out := []string{}
	for _, v := range q.queries {
		out = append(out, v.String())
	}
	s := strings.Join(out, " AND ")
	if q.not != nil {
		s = fmt.Sprintf("%s[-(%s)]", s, q.not.String())
	}
	return s
}

func (q *BoolAndQuery) advance(target int32) int32 {
	if len(q.queries) == 0 {
		q.docId = NO_MORE
		return NO_MORE
	}

	return q.nextAndedDoc(q.queries[0].advance(target))
}

func (q *BoolAndQuery) Next() int32 {
	if len(q.queries) == 0 {
		q.docId = NO_MORE
		return NO_MORE
	}

	// XXX: pick cheapest leading query
	return q.nextAndedDoc(q.queries[0].Next())
}

type BoolAndThenQuery struct {
	A     Query
	B     Query
	delta int32
	QueryBase
}

func NewAndThenQuery(a, b Query, delta int32) *BoolAndThenQuery {
	return &BoolAndThenQuery{
		A:         a,
		B:         b,
		delta:     delta,
		QueryBase: QueryBase{NOT_READY, 0},
	}
}

func (q *BoolAndThenQuery) Score() float32 {
	return 2
}

func (q *BoolAndThenQuery) nextAndedDoc(target int32) int32 {
	if q.docId == target {
		return q.docId
	}
	if q.A.GetDocId() == NOT_READY {
		q.A.Next()
	}
	target = q.B.advance(target)
	for {
		if target == NO_MORE || q.A.GetDocId() == NO_MORE {
			q.docId = NO_MORE
			q.time = 0
			return q.docId
		}

		tB := q.B.GetTime()
		tA := q.A.GetTime()
		diff := (tB - tA)
		if diff > 0 && diff <= q.delta {
			q.docId = target
			q.time = tB
			return q.docId
		}
		if tB > tA {
			q.A.Next()
		} else {
			target = q.B.Next()
		}
	}
}

func (q *BoolAndThenQuery) Reset() {
	q.docId = NOT_READY
	q.time = 0
	q.A.Reset()
	q.B.Reset()
}

func (q *BoolAndThenQuery) String() string {
	return fmt.Sprintf("%s AND THEN(%d) %s", q.A.String(), q.delta, q.B.String())
}

func (q *BoolAndThenQuery) advance(target int32) int32 {
	return q.nextAndedDoc(q.B.advance(target))
}

func (q *BoolAndThenQuery) Next() int32 {
	return q.nextAndedDoc(q.B.Next())
}
