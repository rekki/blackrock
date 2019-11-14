package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

type ErrorBody struct {
	Error string `json:"error"`
}

func tryParseError(data []byte) error {
	e := &ErrorBody{}
	err := json.Unmarshal(data, e)
	if err == nil {
		return errors.New(e.Error)
	} else {
		return errors.New(string(data))
	}
}

type Client struct {
	h                 *http.Client
	endpointFetch     string
	endpointSearch    string
	endpointAggregate string
	cache             *cache.Cache
}

func NewClient(url string, h *http.Client) *Client {
	if h == nil {
		tr := &http.Transport{
			MaxIdleConns:    10,
			IdleConnTimeout: 30 * time.Second,
		}
		h = &http.Client{Transport: tr}
	}
	if !strings.HasSuffix(url, "/") {
		url = url + "/"
	}
	return &Client{
		endpointFetch:     fmt.Sprintf("%sapi/v0/fetch", url),
		endpointSearch:    fmt.Sprintf("%sapi/v0/search", url),
		endpointAggregate: fmt.Sprintf("%sapi/v0/aggregate", url),
		h:                 h,
		cache:             cache.New(48*time.Hour, 10*time.Minute),
	}
}

func (c *Client) Fetch(query *spec.SearchQueryRequest, cb func(*spec.Hit) bool) error {
	data, err := proto.Marshal(query)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.endpointFetch, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := c.h.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 10*1024*1024), 10*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		var decoded spec.Hit
		err := jsonpb.UnmarshalString(line, &decoded)
		if err != nil {
			return err
		}
		if cb(&decoded) {
			return nil
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (c *Client) Aggregate(query *spec.AggregateRequest) (*spec.Aggregate, error) {
	data, err := proto.Marshal(query)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.endpointAggregate, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	resp, err := c.h.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, tryParseError(data)
	}
	agg := &spec.Aggregate{}
	err = proto.Unmarshal(data, agg)
	if err != nil {
		return nil, err
	}
	return agg, nil
}

func (c *Client) Search(query *spec.SearchQueryRequest) (*spec.SearchQueryResponse, error) {
	data, err := proto.Marshal(query)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.endpointSearch, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	resp, err := c.h.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	sq := &spec.SearchQueryResponse{}
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, tryParseError(data)
	}

	err = proto.Unmarshal(data, sq)
	if err != nil {
		return nil, err
	}
	return sq, nil
}

func (c *Client) AggregateCoordinate(qr *spec.AggregateRequest) (*spec.Aggregate, error) {
	dates := depths.ExpandYYYYMMDD(qr.Query.From, qr.Query.To)

	done := make(chan *spec.Aggregate)
	for _, date := range dates {
		go func(date time.Time) {
			segment := depths.SegmentFromNs(date.UnixNano())
			cacheKey := fmt.Sprintf("%s:%s", segment, depths.DumpObjNoIndent(qr.Query.Query))
			cached, found := c.cache.Get(cacheKey)
			if found {
				data := cached.([]byte)
				out := &spec.Aggregate{}
				err := proto.Unmarshal(data, out)
				if err != nil {
					log.WithError(err).Warnf("failed to decode")
				}
				done <- out
				return
			}

			modifiedQueryRequest := *qr
			modifiedQueryDate := *modifiedQueryRequest.Query
			modifiedQueryDate.From = depths.YYYYMMDD(date)
			modifiedQueryDate.To = depths.YYYYMMDD(date)
			modifiedQueryRequest.Query = &modifiedQueryDate
			out, err := c.Aggregate(&modifiedQueryRequest)
			if time.Since(date) > 24*time.Hour {
				data, err := proto.Marshal(out)
				if err != nil {
					log.WithError(err).Warnf("failed to encode")
				} else {
					c.cache.Set(cacheKey, data, cache.DefaultExpiration)
				}
			}
			if err != nil {
				log.WithError(err).Warnf("failed to execute query on segment: %v", segment)
			}
			done <- out
		}(date)
	}

	var merged *spec.Aggregate
	for range dates {
		current := <-done
		merged = merge(merged, current)
	}

	sort.Slice(merged.Sample, func(i, j int) bool {
		return merged.Sample[i].Metadata.CreatedAtNs < merged.Sample[j].Metadata.CreatedAtNs
	})
	if len(merged.Sample) > int(qr.SampleLimit) {
		merged.Sample = merged.Sample[:qr.SampleLimit]
	}
	return merged, nil
}

func mergeMapCountKV(into map[string]*spec.CountPerKV, from map[string]*spec.CountPerKV) map[string]*spec.CountPerKV {
	if into == nil {
		return from
	}
	for k, fk := range from {
		in, ok := into[k]
		if !ok {
			into[k] = fk
			continue
		}
		in.Total += fk.Total
		if in.Count == nil {
			in.Count = fk.Count
		} else {
			for kk, vv := range fk.Count {
				in.Count[kk] += vv
			}
		}
	}
	return into
}

func merge(into *spec.Aggregate, from *spec.Aggregate) *spec.Aggregate {
	if into == nil {
		return from
	}

	/*
		message CountPerKV {
		        map<string, uint32> count = 1;
		        uint32 total = 2;
		        string key = 3;
		}

		message Aggregate {
		        map<string, CountPerKV> search = 1;
		        map<string, CountPerKV> count = 2;
		        map<string, CountPerKV> foreign_id = 3;
		        map<string, CountPerKV> event_type = 4;
		        map<string, uint32> possible = 6;
		        uint32 total = 5;
		}
	*/

	into.Total += from.Total

	for k, v := range from.Possible {
		into.Possible[k] += v
	}

	into.Search = mergeMapCountKV(into.Search, from.Search)
	into.Count = mergeMapCountKV(into.Count, from.Count)
	into.ForeignId = mergeMapCountKV(into.ForeignId, from.ForeignId)
	into.EventType = mergeMapCountKV(into.EventType, from.EventType)
	into.Sample = append(into.Sample, from.Sample...)
	return into
}
