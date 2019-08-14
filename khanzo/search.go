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

func (qr *QueryResponse) VW(c *gin.Context) {
	labels := map[string]int{}

	for i := -1; i < 10; i++ {
		l := c.Query(fmt.Sprintf("label_%d", i))
		if l != "" {
			labels[l] = i
		}
	}
	if len(labels) == 0 {
		c.JSON(400, gin.H{"error": "no labels found, use ?label_1=some_event_type"})
		return
	}
	w := c.Writer
	for _, hit := range qr.Hits {
		m := hit.Metadata
		label, ok := labels[m.EventType]
		if !ok {
			continue
		}

		w.Write([]byte(fmt.Sprintf("%d |%s %s ", label, hit.Metadata.ForeignType, depths.CleanupVW(hit.Metadata.ForeignId))))
		for _, kv := range m.Search {
			w.Write([]byte(fmt.Sprintf("|%s %s ", kv.Key, depths.CleanupVW(kv.Value))))
		}
		for _, kv := range m.Count {
			w.Write([]byte(fmt.Sprintf("|%s %s ", kv.Key, depths.CleanupVW(kv.Value))))
		}
		for _, ctx := range hit.Context {
			for _, kv := range ctx.Properties {
				w.Write([]byte(fmt.Sprintf("|%s_%s %s ", ctx.ForeignType, kv.Key, depths.CleanupVW(kv.Value))))
			}
		}
		w.Write([]byte{'\n'})
	}
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
