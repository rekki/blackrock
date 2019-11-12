package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
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
	}
}

func (c *Client) Fetch(query *spec.SearchQueryRequest, cb func(*spec.Hit)) error {
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
	for scanner.Scan() {
		line := scanner.Text()
		var decoded spec.Hit
		err := jsonpb.UnmarshalString(line, &decoded)
		if err != nil {
			return err
		}
		cb(&decoded)
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
