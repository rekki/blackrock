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
	h             *http.Client
	endpointFetch string
	user          string
	pass          string
}

func NewClient(url, user, pass string, h *http.Client) *Client {
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
	return &Client{endpointFetch: fmt.Sprintf("%sv0/fetch", url), h: h, user: user, pass: pass}
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
	req.SetBasicAuth(c.user, c.pass)
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
