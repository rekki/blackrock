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
func newEnvelope(product string) *Envelope {
	return &Envelope{Metadata: &Metadata{Search: []KV{KV{Key: "product", Value: product}}}}
}
func TestToken(t *testing.T) {
	tp := NewTokenMap("aa:app,aa:mobile,bb:website", "aa:user_id,aa:author_id,cc:book_id")
	at := tp.ExtractFromToken("abc")
	if at.AllowContext(&Context{ForeignType: "book_id"}) {
		t.Fatal("expected to fail")
	}
	if !tp.ExtractFromToken("cc").AllowContext(&Context{ForeignType: "book_id"}) {
		t.Fatal("expected to succeed")
	}
	if tp.ExtractFromToken("cc").AllowContext(&Context{ForeignType: "book_id_wrong"}) {
		t.Fatal("expected to fail")
	}

	if tp.ExtractFromToken("cc").AllowEnvelope(newEnvelope("abc")) {
		t.Fatal("expected to fail")
	}

	if !tp.ExtractFromToken("aa").AllowEnvelope(newEnvelope("app")) {
		t.Fatal("expected to succeed")
	}

	if !tp.ExtractFromToken("aa").AllowEnvelope(newEnvelope("mobile")) {
		t.Fatal("expected to succeed")
	}

	if tp.ExtractFromToken("aa").AllowEnvelope(newEnvelope("website")) {
		t.Fatal("expected to fail")
	}

	if !tp.ExtractFromToken("bb").AllowEnvelope(newEnvelope("website")) {
		t.Fatal("expected to succeed")
	}

}
