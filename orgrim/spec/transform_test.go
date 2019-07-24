package spec

import (
	"testing"
)

func TestTransform(t *testing.T) {
	m := map[string]interface{}{
		"book_id": map[string]interface{}{"123": true},
	}
	tr, err := Transform(m, true)
	if err != nil {
		t.Fatal(err)
	}
	if tr[0].Key != "book_id" && tr[0].Value != "123" {
		t.Fatal("expected book_id:123")
	}
}
