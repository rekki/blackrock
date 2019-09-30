package consume

import (
	"testing"
)

func TestFixme(t *testing.T) {
	cases := []string{
		"a.b.c", "true",
		"a.b_code.c", "true",
		"a.b.c.d.e.b_code.c", "true",
		"a.b.c.d_code.e.b.c", "true",
		"a_code.b.c", "true",
		"a_code.b", "true",
	}

	expected := []string{
		"a.b.c", "true",
		"a.b_code", "c",
		"a.b.c.d.e.b_code", "c",
		"a.b.c.d_code", "e.b.c",
		"a_code", "b.c",
		"a_code", "b",
	}
	for i := 0; i < len(cases); i += 2 {
		k, v := Fixme(cases[i], cases[i+1])
		if k != expected[i] || v != expected[i+1] {
			t.Fatalf("[got]%v %v != [expected]%v %v", k, v, expected[i], expected[i+1])
		}
	}
}

func TestCutInt(t *testing.T) {
	cases := map[string]int{
		"x_b_casd_1": 1,
		"x_1":        1,
		"_1":         0,
		"x_b_c_":     0,
	}

	for k, v := range cases {
		_, e, _ := ExtractLastNumber(k, byte('_'))

		if v != e {
			t.Fatalf("expected %d got %d", v, e)
		}
	}
}
