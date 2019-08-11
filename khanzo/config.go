package main

import (
	"fmt"
	"strings"
)

type DisplayConfig struct {
	SortByName bool `json:"sort_by_name"`
	Hide       bool `json:"sort_by_name"`
}

type ViewConfig struct {
	PerKey map[string]*DisplayConfig
}

func LoadConfigFromString(f string) (*ViewConfig, error) {
	m := map[string]*DisplayConfig{}

	for _, item := range strings.Split(f, "|") {
		if len(item) == 0 {
			continue
		}
		splitted := strings.Split(item, ":")
		if len(splitted) != 3 {
			return nil, fmt.Errorf("bad item %s", item)
		}
		m[splitted[0]] = &DisplayConfig{
			Hide:       splitted[1] == "true",
			SortByName: splitted[1] == "true",
		}
	}
	return &ViewConfig{
		PerKey: m,
	}, nil
}
