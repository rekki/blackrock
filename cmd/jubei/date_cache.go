package main

import (
	"fmt"
	"time"
)

// yes this is 10% improvement
type YM struct {
	year  int
	month int
}

type YMD struct {
	year  int
	month int
	day   int
}

type YMDH struct {
	year  int
	month int
	day   int
	hour  int
}

type DateCache struct {
	y    map[int]string
	ym   map[YM]string
	ymd  map[YMD]string
	ymdh map[YMDH]string
}

func NewDateCache() *DateCache {
	dc := &DateCache{
		y:    map[int]string{},
		ym:   map[YM]string{},
		ymd:  map[YMD]string{},
		ymdh: map[YMDH]string{},
	}
	for year := 2000; year < 2100; year++ {
		dc.y[year] = fmt.Sprintf("%d", year)

		for month := 1; month <= 12; month++ {
			dc.ym[YM{year, month}] = fmt.Sprintf("%d-%02d", year, month)

			for day := 1; day <= 31; day++ {
				dc.ymd[YMD{year, month, day}] = fmt.Sprintf("%d-%02d-%02d", year, month, day)

				for hour := 0; hour < 24; hour++ {
					dc.ymdh[YMDH{year, month, day, hour}] = fmt.Sprintf("%d-%02d-%02d-%02d", year, month, day, hour)
				}
			}
		}
	}

	return dc
}

func (d *DateCache) Expand(t time.Time) (string, string, string, string) {
	year, m, day := t.Date()
	hour, _, _ := t.Clock()
	month := int(m)
	return d.y[year], d.ym[YM{year, month}], d.ymd[YMD{year, month, day}], d.ymdh[YMDH{year, month, day, hour}]
}
