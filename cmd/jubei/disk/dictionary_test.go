package disk

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestDictionary(t *testing.T) {
	dir, err := ioutil.TempDir("", "dictionary")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cases := map[string]uint32{}
	for k := 0; k < 10; k++ {
		d, err := NewPersistedDictionary(dir)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 1000; i++ {
			str := strings.Trim(strings.ToLower(RandStringRunes(i)), " ")
			id, err := d.GetUniqueTerm(str)
			if err != nil {
				t.Fatal(err)
			}

			resolved, ok := d.Resolve(str)
			if !ok {
				t.Fatalf("cant resolve %s", str)
			}
			if resolved != id {
				t.Fatalf("expected %d got %d", id, resolved)
			}
			revResolved := d.ReverseResolve(id)
			if revResolved != str {
				t.Fatalf("expected %s got %s", str, revResolved)
			}
			cases[str] = id
		}
		d.Close()

		d, err = NewPersistedDictionary(dir)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range cases {
			resolved, ok := d.Resolve(k)
			if !ok {
				t.Fatalf("cant resolve %s", k)
			}

			if resolved != v {
				t.Fatalf("expected %d got %d", v, resolved)
			}
			revResolved := d.ReverseResolve(v)
			if revResolved != k {
				t.Fatalf("expected %s got %s", k, revResolved)
			}
		}
		d.Close()
	}
}
