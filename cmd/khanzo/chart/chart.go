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

func Truncate(x string, max int) string {

	if len(x) < max {
		return x
	}

	return x[:max-3] + "..."
}

func Fit(x float64) string {
	div := float64(1)
	var f string
	for _, m := range multipliers {
		f = fmt.Sprintf("%s%s", strconv.FormatFloat(x, 'f', 0, 64), m)
		if len(f) < 8 {
			return f
		}
		div *= float64(1000)
		x /= div

	}

	return f
}

func HorizontalBar(x []float64, y []string, symbol rune, width int, prefix string, size int) string {
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

	for i, v := range y {
		v = Truncate(v, width/2)
		y[i] = v
		if len(v) > maxLabelWidth {
			maxLabelWidth = len(v)
		}
	}

	width -= maxLabelWidth + 10 + 5 + len(prefix)
	lines := []string{}
	for i := 0; i < end; i++ {
		v := x[i]
		label := y[i]

		mustPad := maxLabelWidth - len(label)

		for k := 0; k < mustPad; k++ {
			label += " "
		}
		barWidth := int((v / max) * float64(width))

		bar := makeBar(symbol, barWidth)
		percent := 100 * (v / sum)

		line := fmt.Sprintf("%s%s %8s %6s%% %s", prefix, label, Fit(x[i]), fmt.Sprintf("%.2f", percent), bar)
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
		out += " "
	}
	out += "┐"
	out += "\n"
	out += "│"
	out += " "
	out += s

	for i := 0; i < width-3-len(s); i++ {
		out += "."
	}
	out += "│"
	out += "\n"
	out += "└"
	for i := 0; i < width-2; i++ {
		out += " "
	}
	out += "┘"

	out += "\n"
	return out
}

func BannerLeft(s string) string {
	out := "\n┌"
	out += "\n"
	out += "│"
	out += " "
	out += s
	out += "\n"
	out += "└"
	out += "\n"
	return out
}
