package main

import (
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/rekki/blackrock/cmd/jubei/disk"
	"github.com/rekki/blackrock/cmd/orgrim/spec"
	"github.com/rekki/blackrock/pkg/depths"
	log "github.com/sirupsen/logrus"
)

// hack to backfix some wrongly flattened keys
// this should be some configurable go script
func Fixme(k, v string) (string, string) {
	if v == "true" {
		splitted := strings.Split(k, ".")
		if len(splitted) > 1 {
			for i := 0; i < len(splitted)-1; i++ {
				p := splitted[i]
				if strings.HasSuffix(p, "_code") {
					k = strings.Join(splitted[:i+1], ".")
					v = strings.Join(splitted[i+1:], ".")
					break
				}
			}
		}
	}
	return k, v
}

func ExtractLastNumber(s string, sep byte) (string, int, bool) {
	pos := -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == sep {
			pos = i
			break
		}
	}

	if pos > 0 && pos < len(s)-1 {
		v, err := strconv.ParseInt(s[pos+1:], 10, 32)
		if err != nil {
			return s, 0, false
		}
		return s[:pos], int(v), true
	}
	return s, 0, false
}

func ConsumeEvent(docId uint32, meta *spec.Metadata, inverted *disk.InvertedWriter) error {
	second := int32(meta.CreatedAtNs / 1e9)

	inverted.Append(int32(docId), second, meta.ForeignType, meta.ForeignId)
	inverted.Append(int32(docId), second, "event_type", meta.EventType)
	for _, kv := range meta.Search {
		inverted.Append(int32(docId), second, kv.Key, kv.Value)
	}
	for ex := range meta.Track {
		inverted.Append(int32(docId), second, "__experiment", ex)
	}

	return nil
}

func buildSegment(root string) (int, error) {
	// FIXME(jackdoe): create lock file to know when job is incomplete so we can just delete the whole segment

	l := log.WithField("root", root).WithField("mode", "BUILD")
	ow, err := NewOffsetWriter(path.Join(root, "inverted.offset"), l)
	if err != nil {

		return 0, err
	}
	defer ow.Close()

	storedOffset, err := ow.ReadOrDefault(0)
	if err != nil {

		return 0, err
	}

	fw, err := disk.NewForwardWriter(root, "main")
	if err != nil {

		return 0, err
	}

	count := 0
	inverted := disk.NewInvertedWriter(path.Join(root, "index"))
	did := uint32(storedOffset)
	err = fw.Scan(uint32(storedOffset), func(offset uint32, data []byte) error {
		metadata := &spec.Metadata{}
		err := proto.Unmarshal(data, metadata)
		if err != nil {
			return err
		}

		err = ConsumeEvent(did, metadata, inverted)
		if err != nil {
			return err
		}
		did = offset
		count++
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("error scanning, startOffset: %d, currentOffset: %d, err: %s", storedOffset, did, err)
	}

	err = inverted.Flush()
	if err != nil {
		return 0, fmt.Errorf("error flushing, startOffset: %d, currentOffset: %d, err: %s", storedOffset, did, err)
	}

	err = ow.SetOffset(int64(did))
	if err != nil {
		return 0, err
	}
	if uint32(storedOffset) != did {
		l.Infof("start offset: %d, end offset: %d", storedOffset, did)
	}
	return count, nil
}

func buildEverything(root string, l *log.Entry) error {
	t0 := time.Now()
	days, err := ioutil.ReadDir(path.Join(root))
	if err != nil {
		return err
	}
	work := []string{}
	for _, day := range days {
		if !day.IsDir() || !depths.IsDigit(day.Name()) {
			continue
		}

		_, err := strconv.Atoi(day.Name())
		if err != nil {
			l.Warnf("skipping %s", day.Name())
			continue
		}
		work = append(work, path.Join(root, day.Name()))
	}
	total := 0
	for _, w := range work {
		t0 := time.Now()
		cnt, err := buildSegment(w)
		if err != nil {
			return err
		}
		if cnt > 0 {
			l.Infof("%s took %s for %d documents", w, time.Since(t0), cnt)
		}
		total += cnt
	}
	l.Warnf("%d events indexed took %s", total, time.Since(t0))
	return nil
}
