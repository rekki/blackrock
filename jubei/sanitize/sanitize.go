package sanitize

import (
	"fmt"
	"path"
	"strings"
	"unicode"

	"github.com/dgryski/go-metro"
)

func PathForTag(root string, topic string, tagKey string, tagValue string) (string, string) {
	dir := path.Join(root, topic, Cleanup(tagKey), fmt.Sprintf("metro_32_%d", Hashs(tagKey)%32))
	return dir, fmt.Sprintf("%s.p", Cleanup(tagValue))
}

func Cleanup(s string) string {
	clean := strings.Map(
		func(r rune) rune {
			if r > unicode.MaxLatin1 {
				return -1
			}

			if '0' <= r && r <= '9' {
				return r
			}

			if 'A' <= r && r <= 'Z' {
				return r
			}

			if 'a' <= r && r <= 'z' {
				return r
			}

			if r == ':' || r == '-' || r == '_' {
				return r
			}
			return '_'
		},
		s,
	)
	if len(clean) > 64 {
		// FIXME(jackdoe): not good
		clean = clean[:64]
	}
	return clean
}

func Hash(s []byte) uint64 {
	return metro.Hash64(s, 0)
}

func Hashs(s string) uint64 {
	return metro.Hash64Str(s, 0)
}
