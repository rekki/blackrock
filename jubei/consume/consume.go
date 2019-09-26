package consume

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
	"github.com/spaolacci/murmur3"
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

func ConsumeEvents(segmentId string, envelope *spec.Envelope, forward *disk.ForwardWriter, inverted *disk.InvertedWriter) error {
	meta := envelope.Metadata
	foreignId := depths.Cleanup(strings.ToLower(meta.ForeignId))
	foreignType := depths.Cleanup(strings.ToLower(meta.ForeignType))
	eventType := depths.Cleanup(strings.ToLower(meta.EventType))

	meta.EventType = eventType
	meta.ForeignType = foreignType
	meta.ForeignId = foreignId

	for i, kv := range meta.Search {
		k := kv.Key
		v := kv.Value

		lc := strings.ToLower(k)
		lc, v = Fixme(lc, v)

		lc = depths.Cleanup(lc)
		if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == foreignType || lc == "" {
			continue
		}

		value := depths.Cleanup(strings.ToLower(v))
		if value == "" {
			value = "__empty"
		}
		meta.Search[i] = spec.KV{Key: lc, Value: value}

		if k == "experiment" && strings.HasPrefix(value, "exp_") {
			name, variant, ok := ExtractLastNumber(value, byte('_'))
			if ok {
				if meta.Track == nil {
					meta.Track = map[string]uint32{}
				}
				meta.Track[name] = uint32(variant)
			}
		}
	}
	// add some automatic tags
	{
		ns := meta.CreatedAtNs
		second := ns / 1000000000
		t := time.Unix(int64(second), 0).UTC()
		year, month, day := t.Date()
		hour, _, _ := t.Clock()
		meta.Search = append(meta.Search, spec.KV{Key: "year", Value: fmt.Sprintf("%d", year)})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month", Value: fmt.Sprintf("%d-%02d", year, month)})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day", Value: fmt.Sprintf("%d-%02d-%02d", year, month, day)})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day-hour", Value: fmt.Sprintf("%d-%02d-%02d-%02d", year, month, day, hour)})
	}

	encoded, err := proto.Marshal(envelope.Metadata)
	if err != nil {
		return err
	}

	docId, err := forward.Append(encoded)
	if err != nil {
		return err
	}

	inverted.Append(segmentId, int32(docId), foreignType, foreignId)
	inverted.Append(segmentId, int32(docId), "event_type", eventType)

	for _, kv := range meta.Search {
		inverted.Append(segmentId, int32(docId), kv.Key, kv.Value)
	}
	for ex, _ := range meta.Track {
		inverted.Append(segmentId, int32(docId), "__experiment", ex)
	}

	return nil
}

func ConsumeContext(envelope *spec.Context, forward *disk.ForwardWriter) error {
	if envelope.CreatedAtNs == 0 {
		envelope.CreatedAtNs = time.Now().UnixNano()
	}

	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = forward.Append(encoded)
	if err != nil {
		return err
	}
	return nil
}

func ExpDice(ftype, id, exp string, variants uint32) uint32 {
	h := murmur3.Sum32WithSeed([]byte(ftype), 0) + murmur3.Sum32WithSeed([]byte(id), 0) + murmur3.Sum32WithSeed([]byte(exp), 0)
	return h % variants
}
