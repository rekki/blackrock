package chart

import (
	"fmt"
	"strings"
)

func makeBar(symbol rune, value int) string {
	s := ""
	for i := int(0); i < value; i++ {
		s += string(symbol)
	}
	return s
}

func HorizontalBar(x []float64, y []string, symbol rune, width int) string {
	max := float64(0)
	maxLabelWidth := 0
	for _, v := range x {
		if v > max {
			max = v
		}
	}

	for _, v := range y {
		if len(v) > maxLabelWidth {
			maxLabelWidth = len(v)
		}
	}

	width -= maxLabelWidth - 4
	lines := []string{}
	for i := 0; i < len(x); i++ {
		v := x[i]
		label := y[i]
		value := int((v / max) * float64(width))
		pad := fmt.Sprintf("%d", len(label)-maxLabelWidth)
		bar := makeBar(symbol, value)
		line := fmt.Sprintf("%"+pad+"s %4f %s", label, x[i], bar)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}
