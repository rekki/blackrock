package stat

import "testing"

type testCase struct {
	g    float64
	data []Variant
}

func TestG(t *testing.T) {
	cases := []testCase{
		testCase{
			g: 0,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 100, Convertions: 10},
			},
		},
		testCase{
			g: 199.4197765579355,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 100, Convertions: 99},
			},
		},
		testCase{
			g: 547.0055631678127,
			data: []Variant{
				Variant{Visits: 100, Convertions: 10},
				Variant{Visits: 1000, Convertions: 999},
			},
		},
	}
	for _, c := range cases {
		g := G(c.data)
		if g != c.g {
			t.Fatalf("expected %v got %v", c.g, g)
		}
	}

}
