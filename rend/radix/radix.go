package radix

import (
	"fmt"

	"github.com/jackdoe/blackrock/orgrim/spec"
)

type node struct {
	v   *spec.Context
	key uint64
	nei [16]*node
}

func NewTree() *node {
	return newNode(0, nil)
}

func newNode(key uint64, v *spec.Context) *node {
	return &node{v: v, nei: [16]*node{}, key: key}
}

func (n *node) Print(prefix string) {
	fmt.Printf("%skey: %d %v\n", prefix, n.key, n.v)
	for i := 0; i < 16; i++ {
		e := n.nei[i]
		if e != nil {
			e.Print(fmt.Sprintf("%s [%d] ", prefix, i))
		}
	}
}

func (n *node) Add(key uint64, v *spec.Context) {
	if key == 0 {
		panic("0 key")
	}

	current := n
	for bits := int64(60); bits >= 0; bits -= 4 {
		nibble := (key >> uint64(bits)) & uint64(0xF)
		next := current.nei[nibble]
		if next == nil {
			next = newNode(key, v)
			current.nei[nibble] = next
		}

		current = next
	}
}

func (n *node) Find(key uint64) *spec.Context {
	if key == 0 {
		panic("0 key")
	}
	current := n
	for bits := int64(60); bits >= 0; bits -= 4 {
		nibble := (key >> uint64(bits)) & uint64(0xF)

		next := current.nei[nibble]
		if next != nil {
			current = next
		} else {
			break
		}
	}

	return current.v
}
