package disk

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/jackdoe/blackrock/depths"
	log "github.com/sirupsen/logrus"
)

func DeleteUncompactedPostings(root string) error {
	fields, err := ioutil.ReadDir(root)
	if err != nil {
		return err
	}

	for _, field := range fields {
		if !field.IsDir() {
			continue
		}
		p := path.Join(root, field.Name())
		log.Warnf("removing %s", p)
		err := os.RemoveAll(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func ReadAllTermsInSegment(root string) (map[string][]uint32, error) {
	fields, err := ioutil.ReadDir(path.Join(root))
	if err != nil {
		return nil, err
	}

	segment := map[string][]uint32{}
	for _, field := range fields {
		if !field.IsDir() {
			continue
		}

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

func WriteCompactedIndex(root string, segment map[string][]uint32) error {
	offsets := map[string]uint32{}
	dw, err := NewForwardWriter(root, "segment.data")
	if err != nil {
		return err
	}
	defer dw.Close()
	for term, postings := range segment {
		off, err := dw.Append(depths.UintsToBytes(postings))
		if err != nil {
			return err
		}
		offsets[term] = off
	}

	encoded, err := json.Marshal(offsets)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path.Join(root, "segment.header.temp"), encoded, 0600)
	if err != nil {
		return err
	}
	err = os.Rename(path.Join(root, "segment.header.temp"), path.Join(root, "segment.header"))
	if err != nil {
		return err
	}

	return err
}

type CompactIndexCache struct {
	sync.RWMutex
	offsets map[string]map[string]uint32
}

func NewCompactIndexCache() *CompactIndexCache {
	return &CompactIndexCache{
		offsets: map[string]map[string]uint32{},
	}
}

func (c *CompactIndexCache) FindPostingsList(root, k, v string) []int32 {
	tagKey := depths.Cleanup(strings.ToLower(k))
	tagValue := depths.Cleanup(strings.ToLower(v))
	headerPath := path.Join(root, "segment.header")
	if _, err := os.Stat(headerPath); os.IsNotExist(err) {
		return InvertedReadRaw(root, -1, tagKey, tagValue)
	}

	c.RLock()
	offsets, ok := c.offsets[root]
	c.RUnlock()
	if !ok {
		header, err := ioutil.ReadFile(headerPath)
		if err != nil {
			// invariant
			log.Warnf("failed to read header, err: %s", err.Error())
			return []int32{}
		}

		offsets = map[string]uint32{}
		err = json.Unmarshal(header, &offsets)
		if err != nil {
			// invariant
			log.Warnf("failed to decode header, err: %s", err.Error())
			return []int32{}
		}

		c.Lock()
		c.offsets[root] = offsets
		c.Unlock()
	}

	offset, ok := offsets[fmt.Sprintf("%s:%s", tagKey, tagValue)]
	if !ok {
		return []int32{}
	}
	fw, err := NewForwardWriter(root, "segment.data")
	if err != nil {
		// invariant
		log.Warnf("failed to open data, err: %s", err.Error())
		return []int32{}
	}
	defer fw.Close()
	data, _, err := fw.Read(offset)
	if err != nil {
		// invariant
		log.Warnf("failed to read data, err: %s", err.Error())
		return []int32{}
	}

	return depths.BytesToInts(data)
}
