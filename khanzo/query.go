package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

const (
	NO_MORE   = int64(math.MaxInt64)
	NOT_READY = int64(-1)
)

// FIXME(jackdoe): this is very bad
func fromString(text string, makeTermQuery func(string, string) Query) (Query, int, error) {
	top := []Query{}
	not := []Query{}
	for _, and := range strings.Split(text, " AND ") {
		sub := []Query{}
		and = strings.Trim(and, " ")
		if and == "" {
			continue
		}
		for _, or := range strings.Split(and, " OR ") {
			or = strings.Trim(or, " ")
			if or == "" {
				continue
			}
			t := strings.SplitN(or, ":", 2)

			if strings.HasPrefix(t[0], "-") && len(t[0]) > 1 {
				term := makeTermQuery(t[0][1:], t[1])
				not = append(not, term)
			} else {
				term := makeTermQuery(t[0], t[1])
				sub = append(sub, term)
			}
		}
		if len(sub) > 0 {
			if len(sub) == 1 {
				top = append(top, sub[0])
			} else {
				top = append(top, NewBoolOrQuery(sub...))
			}
		}
	}
	if (len(top)) == 1 && len(not) == 0 {
		return top[0], len(top), nil
	}
	if len(not) > 0 {
		return NewBoolAndNotQuery(NewBoolOrQuery(not...), top...), len(top), nil
	}
	return NewBoolAndQuery(top...), len(top), nil
}

func expandYYYYMMDD(from string, to string, makeTermQuery func(string, string) Query) Query {
	fromTime := time.Now().UTC().AddDate(0, 0, -3)
	toTime := time.Now().UTC()

	if from != "" {
		d, err := time.Parse("2006-01-02", from)
		if err == nil {
			fromTime = d
		}
	}
	if to != "" {
		d, err := time.Parse("2006-01-02", to)
		if err == nil {
			toTime = d
		}
	}
	dateQuery := []Query{}
	start := fromTime.AddDate(0, 0, 0)
	for {
		dateQuery = append(dateQuery, makeTermQuery("year-month-day", yyyymmdd(start)))
		start = start.AddDate(0, 0, 1)
		if start.Sub(toTime) > 0 {
			break
		}
	}
	return NewBoolOrQuery(dateQuery...)
}

/*

{
   and: [{"or": [{"tag":{"key":"b","value": "v"}}]}]
}

*/

func fromJson(input interface{}, makeTermQuery func(string, string) Query) (Query, int, error) {
	mapped, ok := input.(map[string]interface{})
	queries := []Query{}
	if ok {
		if v, ok := mapped["tag"]; ok && v != nil {
			kv, ok := v.(map[string]interface{})
			if !ok {
				return nil, 0, errors.New("[tag] must be map containing {key, value}")
			}
			added := false
			if tk, ok := kv["key"]; ok && tk != nil {
				if tv, ok := kv["value"]; ok && tv != nil {
					sk, ok := tk.(string)
					if !ok {
						return nil, 0, errors.New("[tag][key] must be string")
					}
					sv, ok := tv.(string)
					if !ok {
						return nil, 0, errors.New("[tag][value] must be string")
					}
					queries = append(queries, makeTermQuery(sk, sv))
					added = true
				}
			}
			if !added {
				return nil, 0, errors.New("[tag] must be map containing {key, value}")
			}
		}
		if v, ok := mapped["text"]; ok && v != nil {
			sk, ok := v.(string)
			if !ok {
				return nil, 0, errors.New("[text] must be string")
			}

			q, _, err := fromString(sk, makeTermQuery)
			if err != nil {
				return nil, 0, err
			}

			queries = append(queries, q)
		}
		if v, ok := mapped["and"]; ok && v != nil {
			list, ok := v.([]interface{})
			if ok {
				and := NewBoolAndQuery([]Query{}...)
				for _, subQuery := range list {
					q, _, err := fromJson(subQuery, makeTermQuery)
					if err != nil {
						return nil, 0, err
					}
					and.AddSubQuery(q)
				}
				queries = append(queries, and)
			} else {
				return nil, 0, errors.New("[or] takes array of subqueries")
			}
		}

		if v, ok := mapped["or"]; ok && v != nil {
			list, ok := v.([]interface{})
			if ok {
				or := NewBoolOrQuery([]Query{}...)
				for _, subQuery := range list {
					q, _, err := fromJson(subQuery, makeTermQuery)
					if err != nil {
						return nil, 0, err
					}
					or.AddSubQuery(q)
				}
				queries = append(queries, or)
			} else {
				return nil, 0, errors.New("[and] takes array of subqueries")
			}
		}
	}

	if len(queries) == 1 {
		return queries[0], len(queries), nil
	}

	return NewBoolAndQuery(queries...), len(queries), nil
}

type Query interface {
	advance(int64) int64
	Next() int64
	GetDocId() int64
	Score() float32
	Reset()
	String() string
}

type QueryBase struct {
	docId int64
}

func (q *QueryBase) GetDocId() int64 {
	return q.docId
}

type Term struct {
	cursor   int
	postings []int64
	term     string
	QueryBase
}

func NewTerm(t string, postings []int64) *Term {
	return &Term{
		term:      t,
		cursor:    -1,
		postings:  postings,
		QueryBase: QueryBase{NOT_READY},
	}
}

func (t *Term) String() string {
	return t.term
}

func (t *Term) Reset() {
	t.cursor = -1
	t.docId = NOT_READY
}

func (t *Term) Score() float32 {
	return float32(1)
}

func (t *Term) advance(target int64) int64 {
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
		current := t.postings[mid]
		if current == target {
			t.cursor = mid
			t.docId = target
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
		return NO_MORE
	}
	t.cursor = start
	t.docId = t.postings[start]
	return t.docId
}

func (t *Term) Next() int64 {
	t.cursor++
	if t.cursor >= len(t.postings) {
		t.docId = NO_MORE
	} else {
		t.docId = t.postings[t.cursor]
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
		QueryBase:     QueryBase{NOT_READY},
	}
}
func (q *BoolOrQuery) Reset() {
	q.docId = NOT_READY
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

func (q *BoolOrQuery) advance(target int64) int64 {
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

func (q *BoolOrQuery) Next() int64 {
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
		QueryBase:     QueryBase{NOT_READY},
	}
}

func (q *BoolAndQuery) SetNot(not Query) *BoolAndQuery {
	q.not = not
	return q
}

func (q *BoolAndQuery) Score() float32 {
	return float32(len(q.queries))
}

func (q *BoolAndQuery) nextAndedDoc(target int64) int64 {
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

func (q *BoolAndQuery) advance(target int64) int64 {
	if len(q.queries) == 0 {
		q.docId = NO_MORE
		return NO_MORE
	}

	return q.nextAndedDoc(q.queries[0].advance(target))
}

func (q *BoolAndQuery) Next() int64 {
	if len(q.queries) == 0 {
		q.docId = NO_MORE
		return NO_MORE
	}

	// XXX: pick cheapest leading query
	return q.nextAndedDoc(q.queries[0].Next())
}

type TermFile struct {
	cursor int64
	size   int64
	file   *os.File
	term   string
	QueryBase
}

func NewTermFile(t string, f *os.File) *TermFile {
	fs, err := f.Stat()
	size := int64(0)
	if err == nil {
		size = fs.Size()
	}
	return &TermFile{
		term:      t,
		cursor:    -1,
		size:      size / 8,
		file:      f,
		QueryBase: QueryBase{NOT_READY},
	}
}

func (t *TermFile) String() string {
	return t.term
}

func (t *TermFile) Reset() {
	t.cursor = -1
	t.docId = NOT_READY
}

func (t *TermFile) Score() float32 {
	return float32(1)
}

func (t *TermFile) read(pos int64) int64 {
	data := make([]byte, 8)
	_, err := t.file.ReadAt(data, pos)
	if err != nil {
		return NO_MORE
	}
	return int64(binary.LittleEndian.Uint64(data))
}

func (t *TermFile) advance(target int64) int64 {
	// FIXME(jackdoe): add buffer with few hundred integers to dramatically reduce the reads
	if t.docId == NO_MORE || t.docId == target || target == NO_MORE {
		t.docId = target
		return t.docId
	}

	if t.cursor < 0 {
		t.cursor = 0
	}

	start := t.cursor
	end := t.size

	for start < end {
		mid := start + ((end - start) / 2)
		current := t.read(mid * 8)
		if current == target {
			t.cursor = mid
			t.docId = target
			return target
		}

		if current < target {
			start = mid + 1
		} else {
			end = mid
		}
	}
	if start >= t.size {
		t.docId = NO_MORE
		return NO_MORE
	}
	t.cursor = start
	t.docId = t.read(start * 8)
	return t.docId
}

func (t *TermFile) Next() int64 {
	t.cursor++
	t.docId = t.read(t.cursor * 8)
	return t.docId
}
