package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/orgrim/spec"
)

type Client struct {
	h                *http.Client
	endpointEnvelope string
	endpointContext  string
}

func NewClient(url string, h *http.Client) *Client {
	if h == nil {
		tr := &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: true, // assume input is already compressed
		}
		h = &http.Client{Transport: tr}
	}
	if !strings.HasSuffix(url, "/") {
		url = url + "/"
	}
	return &Client{endpointEnvelope: fmt.Sprintf("%spush/envelope", url), endpointContext: fmt.Sprintf("%spush/context", url), h: h}
}

type success struct {
	Success bool `json:"success"`
}

func (c *Client) push(endpoint string, message proto.Message) error {
	blob, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	resp, err := c.h.Post(endpoint, "application/protobuf", bytes.NewReader(blob))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var s success
	err = json.Unmarshal(body, &s)
	if err != nil {
		return err
	}

	if s.Success != true {
		return fmt.Errorf("expected {success:true} got '%v'", s)
	}
	return nil
}

func (c *Client) Push(envelope *spec.Envelope) error {
	return c.push(c.endpointEnvelope, envelope)
}

func (c *Client) PushContext(message *spec.Context) error {
	return c.push(c.endpointContext, message)
}
