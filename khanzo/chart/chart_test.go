package chart

import (
	"fmt"
	"testing"
)

func TestEverything(t *testing.T) {
	cases := []struct {
		x        []float64
		y        []string
		width    int
		symbol   rune
		expected string
	}{

		{
			[]float64{1, 2, 3, 4},
			[]string{"a", "b", "c", "d"},
			80,
			'#',
			`a      1.0  10.00% ###############
b      2.0  20.00% ##############################
c      3.0  30.00% #############################################
d      4.0  40.00% #############################################################`},

		{
			[]float64{1, 2, 3, 4},
			[]string{"a", "b", "c", "d"},
			30,
			'#',
			`a      1.0  10.00% ##
b      2.0  20.00% #####
c      3.0  30.00% ########
d      4.0  40.00% ###########`},
	}

	for i := range cases {
		name := fmt.Sprintf("%d", i)
		t.Run(name, func(t *testing.T) {
			c := cases[i]
			actual := HorizontalBar(c.x, c.y, c.symbol, c.width, "")
			if actual != c.expected {
				t.Errorf("Plot(%#v)", c)
				t.Logf("expected:\n%s\n", c.expected)
			}
			t.Logf("actual:\n%s\n", actual)
		})
	}

}
