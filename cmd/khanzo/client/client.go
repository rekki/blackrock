package client

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
)

type Client struct {
	h             *http.Client
	endpointFetch string
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
	return &Client{endpointFetch: fmt.Sprintf("%sv0/fetch", url), h: h}
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
