package main

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/mmcloughlin/geohash"
	"github.com/thomersch/gosmparse"
)

type Location struct {
	City     string `json:"city"`
	GH5      string `json:"gh5"`
	GH6      string `json:"gh6"`
	GH7      string `json:"gh7"`
	ID       string `json:"id"`
	MatchAll string `json:"match_all"`
}

const MatchAllValue = "blackrock_true"

type dataHandler struct {
	cb func(*Location)
}

func (d *dataHandler) ReadWay(w gosmparse.Way)           {}
func (d *dataHandler) ReadRelation(r gosmparse.Relation) {}
func (d *dataHandler) ReadNode(n gosmparse.Node) {
	_, ok := n.Tags["addr:city"]
	if !ok {
		return
	}

	loc := &Location{}
	v, ok := n.Tags["addr:city"]
	if ok {
		loc.City = strings.ToLower(v)
	}
	loc.GH5 = geohash.EncodeWithPrecision(n.Lat, n.Lon, 5)
	loc.GH6 = geohash.EncodeWithPrecision(n.Lat, n.Lon, 6)
	loc.GH7 = geohash.EncodeWithPrecision(n.Lat, n.Lon, 7)
	loc.MatchAll = MatchAllValue
	loc.ID = fmt.Sprintf("%d", n.ID)
	d.cb(loc)
}

func OSMDecoder(cb func(*Location)) {
	fn := os.Getenv("OSM_DATA")
	if fn == "" {
		fn = "Amsterdam.osm.pbf"
	}
	r, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	dec := gosmparse.NewDecoder(r)
	counter := uint64(0)
	lock := sync.Mutex{}
	dh := &dataHandler{cb: func(l *Location) {
		lock.Lock()
		defer lock.Unlock()
		counter++
		cb(l)
	}}
	err = dec.Parse(dh)
	if err != nil {
		panic(err)
	}
}
