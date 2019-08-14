package disk

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

func ReadAllTermsInSegment(root string) (map[string][]uint32, error) {
	fields, err := ioutil.ReadDir(path.Join(root))
	if err != nil {
		return nil, err
	}

	segment := map[string][]uint32{}
	for _, field := range fields {
		shards, err := ioutil.ReadDir(path.Join(root, field.Name()))
		if err != nil {
			return nil, err
		}

		for _, shardDir := range shards {
			if strings.HasPrefix(shardDir.Name(), "shard_") {
				lists, err := ioutil.ReadDir(path.Join(root, field.Name(), shardDir.Name()))
				if err != nil {
					return nil, err
				}

				for _, term := range lists {
					if strings.HasSuffix(term.Name(), ".p") {
						file, err := os.OpenFile(path.Join(root, field.Name(), shardDir.Name(), term.Name()), os.O_RDONLY, 0600)
						if err != nil {
							return nil, err
						}

						postings, err := ioutil.ReadAll(file)
						n := len(postings) / 4
						longed := make([]uint32, n)
						j := 0
						for i := 0; i < n*4; i += 4 {
							longed[j] = binary.LittleEndian.Uint32(postings[i:])
							j++
						}
						t := strings.TrimSuffix(term.Name(), ".p")
						segment[fmt.Sprintf("%s:%s", field.Name(), t)] = longed
						file.Close()
					}
				}
			}
		}
	}
	return segment, nil
}
