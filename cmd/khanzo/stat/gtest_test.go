package stat

import (
	"math"
	"testing"
)

type testCase struct {
	g           float64
	p           float64
	significant bool
	data        []Variant
}

func same(a, b, d float64) bool {
	return math.Abs(a-b) <= d
}

func TestG(t *testing.T) {
	cases := []testCase{
		testCase{
			g:           0,
			significant: false,
			p:           1,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 100, Convertions: 10},
			},
		},
		testCase{
			g:           3.9,
			significant: true,
			p:           0,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 100, Convertions: 20},
			},
		},
		testCase{
			g:           0.2,
			significant: false,
			p:           0.65,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 100, Convertions: 12},
			},
		},
		testCase{
			g:           199.4197765579355,
			significant: true,
			p:           0,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 100, Convertions: 99},
			},
		},
		testCase{
			g:           547.0055631678127,
			significant: true,
			p:           0,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 1000, Convertions: 999},
			},
		},
	}

	for _, c := range cases {
		g := G(c.data)
		if !same(g, c.g, 0.1) {
			t.Fatalf("g-test: expected %+v got %v", c.g, g)
		}
		p, _, s := P(c.data)
		if !same(p, c.p, 0.1) {
			t.Fatalf("p-value: expected %+v got %v", c.p, p)
		}
		if s != c.significant {
			t.Logf("significant: expected %+v got %v", c, s)
		}

	}

}
