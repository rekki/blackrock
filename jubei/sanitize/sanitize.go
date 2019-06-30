package sanitize

import (
	"fmt"
	"path"
	"strings"
	"unicode"
)

func PathForTag(root string, topic string, partition int32, tagKey string, tagValue string) (string, string) {
	dir := path.Join(root, topic, fmt.Sprintf("%d", partition), Cleanup(tagKey))

	return dir, fmt.Sprintf("%s.p", Cleanup(tagValue))
}

func Cleanup(s string) string {
	return strings.Map(
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
}
