package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rekki/blackrock/cmd/orgrim/spec"
)

type ClientQueryRequest struct {
	Query map[string]interface{} `json:"query"`
	Size  int                    `json:"size"`
	From  string                 `json:"from"`
	To    string                 `json:"to"`
}

type Hit struct {
	Score    float32       `json:"score,omitempty"`
	ID       uint64        `json:"id,omitempty"`
	Metadata spec.Metadata `json:"metadata,omitempty"`
}

type Client struct {
	h                  *http.Client
	endpointFetch      string
	endpointSetContext string
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
	return &Client{endpointFetch: fmt.Sprintf("%sv0/fetch", url), endpointSetContext: fmt.Sprintf("%sstate/set", url), h: h}
}

func (c *Client) PushState(ctx []*spec.Context) error {
	data, err := json.Marshal(ctx)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.endpointSetContext, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.h.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("got status %d, expected 200", resp.StatusCode)
	}
	return nil
}

func (c *Client) Fetch(query *ClientQueryRequest, cb func(*Hit)) error {
	data, err := json.Marshal(query)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.endpointFetch, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.h.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		var decoded Hit
		err := json.Unmarshal([]byte(line), &decoded)
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
