package main

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/disk"
)

type ExpQueryRequest struct {
	Exp           string         `json:"exp"`
	Cohort        map[string]int `json:"cohort"`
	Query         interface{}    `json:"query"`
	Variants      int            `json:"variants"`
	ExperimentKey string         `json:"key"`
	From          string         `json:"from"`
	To            string         `json:"to"`
}

type QueryRequest struct {
	Query interface{} `json:"query"`
	Size  int         `json:"size"`
	From  string      `json:"from"`
	To    string      `json:"to"`
}

type QueryResponse struct {
	Total int64 `json:"total"`
	Hits  []Hit `json:"hits"`
}

func (qr *QueryResponse) String(c *gin.Context) {
	c.YAML(200, qr)
}

func (qr *QueryResponse) HTML(c *gin.Context) {
	c.YAML(200, qr)
}

func NewTermQuery(root string, tagKey string, tagValue string) Query {
	tagKey = depths.Cleanup(strings.ToLower(tagKey))
	tagValue = depths.Cleanup(strings.ToLower(tagValue))
	s := fmt.Sprintf("%s:%s", tagKey, tagValue)
	return NewTerm(s, disk.InvertedReadRaw(root, -1, tagKey, tagValue))
}
