package disk

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

func ReadAllTermsInSegment(root string, segmentId string) (map[string][]uint64, error) {
	fields, err := ioutil.ReadDir(path.Join(root, segmentId))
	if err != nil {
		return nil, err
	}
	segment := map[string][]uint64{}
	for _, field := range fields {
		shards, err := ioutil.ReadDir(path.Join(root, segmentId, field.Name()))
		if err != nil {
			return nil, err
		}

		for _, shardDir := range shards {
			if strings.HasPrefix(shardDir.Name(), "shard_") {
				lists, err := ioutil.ReadDir(path.Join(root, segmentId, field.Name(), shardDir.Name()))
				if err != nil {
					return nil, err
				}

				for _, term := range lists {
					if strings.HasSuffix(term.Name(), ".p") {
						file, err := os.OpenFile(path.Join(root, segmentId, field.Name(), shardDir.Name(), term.Name()), os.O_RDONLY, 0600)
						if err != nil {
							return nil, err
						}

						postings, err := ioutil.ReadAll(file)
						n := len(postings) / 8
						longed := make([]uint64, n)
						j := 0
						for i := 0; i < n*8; i += 8 {
							longed[j] = binary.LittleEndian.Uint64(postings[i:])
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
