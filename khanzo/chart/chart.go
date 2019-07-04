package chart

import (
	"fmt"
	"strconv"
	"strings"
)

func makeBar(symbol rune, value int) string {
	s := ""
	for i := int(0); i < value; i++ {
		s += string(symbol)
	}
	return s
}

var multipliers = []string{"", "k", "M", "G", "T", "P"}

func fit(x float64) string {
	div := float64(1)
	var f string
	for _, m := range multipliers {
		f = fmt.Sprintf("%s%s", strconv.FormatFloat(x, 'f', 1, 64), m)
		if len(f) < 8 {
			return f
		}
		div *= float64(1000)
		x /= div

	}

	return f
}

type Label struct {
	Display string
	Len     int
}

func HorizontalBar(x []float64, y []Label, symbol rune, width int, prefix string, size int) string {
	max := float64(0)
	maxLabelWidth := 0
	sum := float64(0)
	for _, v := range x {
		sum += v
	}
	end := len(x)
	if size > 0 && size < len(x) {
		end = size
	}
	for i := 0; i < end; i++ {
		if x[i] > max {
			max = x[i]
		}
	}
	for _, v := range y {
		if v.Len > maxLabelWidth {
			maxLabelWidth = v.Len
		}
	}

	width -= maxLabelWidth + 10 + 8
	lines := []string{}
	for i := 0; i < end; i++ {
		v := x[i]
		label := y[i].Display
		mustPad := maxLabelWidth - y[i].Len
		for k := 0; k < mustPad; k++ {
			label += " "
		}
		value := int((v / max) * float64(width))

		bar := makeBar(symbol, value)
		percent := 100 * (v / sum)

		line := fmt.Sprintf("%s%s %8s %6s%% %s", prefix, label, fit(x[i]), fmt.Sprintf("%.2f", percent), bar)
		lines = append(lines, line)
	}
	if end < len(x) {
		line := fmt.Sprintf("%s....... skipping %d", prefix, len(x)-end)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func Banner(s string) string {
	width := 80
	out := "\n┌"
	for i := 0; i < width-2; i++ {
		out += "─"
	}
	out += "┐"
	out += "\n"
	out += "│"
	out += " "
	out += s

	for i := 0; i < width-3-len(s); i++ {
		out += " "
	}
	out += "│"
	out += "\n"
	out += "└"
	for i := 0; i < width-2; i++ {
		out += "─"
	}
	out += "┘"

	out += "\n"
	return out
}
