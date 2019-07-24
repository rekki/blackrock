package consume

import "testing"

func TestFixme(t *testing.T) {
	cases := []string{
		"a.b.c", "true",
		"a.b_code.c", "true",
		"a.b.c.d.e.b_code.c", "true",
		"a_code.b.c", "true",
	}

	expected := []string{
		"a.b.c", "true",
		"a.b_code", "c",
		"a.b.c.d.e.b_code", "c",
		"a_code.b.c", "true",
	}
	for i := 0; i < len(cases); i += 2 {
		k, v := Fixme(cases[i], cases[i+1])
		if k != expected[i] || v != expected[i+1] {
			t.Fatalf("%v %v != %v %v", k, v, expected[i], expected[i+1])
		}
	}
}
