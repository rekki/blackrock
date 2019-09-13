package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/orgrim/spec"
)

type Client struct {
	h                *http.Client
	endpointEnvelope string
	endpointContext  string
	token            string
}

func NewClient(url string, token string, h *http.Client) *Client {
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
	return &Client{token: token, endpointEnvelope: fmt.Sprintf("%spush/envelope", url), endpointContext: fmt.Sprintf("%spush/context", url), h: h}
}

type success struct {
	Success bool `json:"success"`
}

func (c *Client) push(endpoint string, message proto.Message) error {
	blob, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(blob))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/protobuf")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.h.Do(req)
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
func ToString(v interface{}) string {
	var value string
	switch v.(type) {
	case string:
		value = v.(string)
	case int:
		value = fmt.Sprintf("%d", v.(int))
	case int32:
		value = fmt.Sprintf("%d", v.(int32))
	case []byte:
		value = string(v.([]byte))
	case int64:
		value = fmt.Sprintf("%d", v.(int64))
	case int16:
		value = fmt.Sprintf("%d", v.(int16))
	case uint32:
		value = fmt.Sprintf("%d", v.(uint32))
	case uint64:
		value = fmt.Sprintf("%d", v.(uint64))
	case uint16:
		value = fmt.Sprintf("%d", v.(uint16))
	case float32:
		value = strconv.FormatFloat(float64(v.(float32)), 'f', 0, 64)
	case float64:
		value = strconv.FormatFloat(v.(float64), 'f', 0, 64)
	case nil:
		value = "nil"
	default:
		value = fmt.Sprintf("%v", v)
	}

	return value
}

func KV(key string, v interface{}) spec.KV {
	value := ToString(v)
	return spec.KV{Key: key, Value: value}
}
