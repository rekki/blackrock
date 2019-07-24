package consume

import (
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackdoe/blackrock/depths"
	"github.com/jackdoe/blackrock/jubei/disk"
	"github.com/jackdoe/blackrock/orgrim/spec"
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

func ConsumeEvents(partition uint32, offset uint64, envelope *spec.Envelope, dictionary *disk.PersistedDictionary, forward *disk.ForwardWriter, payload *disk.ForwardWriter, inverted *disk.InvertedWriter) error {
	typeKey, err := dictionary.GetUniqueTerm("event_type")
	if err != nil {
		return err
	}

	yearKey, err := dictionary.GetUniqueTerm("year")
	if err != nil {
		return err
	}
	yearMonthKey, err := dictionary.GetUniqueTerm("year-month")
	if err != nil {
		return err
	}

	yearMonthDayKey, err := dictionary.GetUniqueTerm("year-month-day")
	if err != nil {
		return err
	}

	yearMonthDayHourKey, err := dictionary.GetUniqueTerm("year-month-day-hour")
	if err != nil {
		return err
	}

	if envelope.Metadata.CreatedAtNs == 0 {
		envelope.Metadata.CreatedAtNs = time.Now().UnixNano()
	}

	meta := envelope.Metadata
	sforeignId := depths.Cleanup(strings.ToLower(meta.ForeignId))
	foreignId, err := dictionary.GetUniqueTerm(sforeignId)
	if err != nil {
		return err
	}
	sforeignType := depths.Cleanup(strings.ToLower(meta.ForeignType))
	foreignType, err := dictionary.GetUniqueTerm(sforeignType)
	if err != nil {
		return err
	}
	poff := uint64(0)
	if payload != nil {
		poff, err = payload.Append(foreignId, foreignType, envelope.Payload)
		if err != nil {
			return err
		}
	}

	seventType := depths.Cleanup(strings.ToLower(meta.EventType))
	eventType, err := dictionary.GetUniqueTerm(seventType)
	if err != nil {
		return err
	}

	persisted := &spec.PersistedMetadata{
		Partition:    uint32(partition),
		Offset:       uint64(offset),
		CreatedAtNs:  envelope.Metadata.CreatedAtNs,
		SearchKeys:   []uint64{},
		CountKeys:    []uint64{},
		PropertyKeys: []uint64{},
		Payload:      poff,
		EventType:    eventType,
		ForeignType:  foreignType,
		ForeignId:    sforeignId,
	}

	for _, kv := range meta.Search {
		k := kv.Key
		v := kv.Value
		lc := depths.CleanupAllowDot(strings.ToLower(k))
		if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == sforeignType || lc == "" {
			continue
		}

		lc, v = Fixme(lc, v)

		tk, err := dictionary.GetUniqueTerm(lc)
		if err != nil {
			return err
		}
		persisted.SearchKeys = append(persisted.SearchKeys, tk)
		value := depths.Cleanup(strings.ToLower(v))
		if value == "" {
			value = "__empty"
		}
		persisted.SearchValues = append(persisted.SearchValues, value)
	}

	// add some automatic tags
	{
		ns := meta.CreatedAtNs
		second := ns / 1000000000
		t := time.Unix(second, 0).UTC()
		year, month, day := t.Date()
		hour, _, _ := t.Clock()

		persisted.SearchKeys = append(persisted.SearchKeys, yearKey)
		persisted.SearchValues = append(persisted.SearchValues, fmt.Sprintf("%d", year))

		persisted.SearchKeys = append(persisted.SearchKeys, yearMonthKey)
		persisted.SearchValues = append(persisted.SearchValues, fmt.Sprintf("%d-%02d", year, month))

		persisted.SearchKeys = append(persisted.SearchKeys, yearMonthDayKey)
		persisted.SearchValues = append(persisted.SearchValues, fmt.Sprintf("%d-%02d-%02d", year, month, day))

		persisted.SearchKeys = append(persisted.SearchKeys, yearMonthDayHourKey)
		persisted.SearchValues = append(persisted.SearchValues, fmt.Sprintf("%d-%02d-%02d-%02d", year, month, day, hour))
	}

	for _, kv := range meta.Count {
		k := kv.Key
		v := kv.Value
		lc := depths.CleanupAllowDot(strings.ToLower(k))
		if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == sforeignType || lc == "" {
			continue
		}

		pk, err := dictionary.GetUniqueTerm(lc)
		if err != nil {
			return err
		}

		value := depths.Cleanup(strings.ToLower(v))
		if value == "" {
			value = "__empty"
		}

		persisted.CountKeys = append(persisted.CountKeys, pk)
		persisted.CountValues = append(persisted.CountValues, value)
	}

	for _, kv := range meta.Properties {
		k := kv.Key
		v := kv.Value
		lc := depths.CleanupAllowDot(strings.ToLower(k))
		if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == sforeignType || lc == "" {
			continue
		}

		pk, err := dictionary.GetUniqueTerm(lc)
		if err != nil {
			return err
		}

		persisted.PropertyKeys = append(persisted.PropertyKeys, pk)
		persisted.PropertyValues = append(persisted.PropertyValues, v)
	}

	encoded, err := proto.Marshal(persisted)
	if err != nil {
		return err
	}

	docId, err := forward.Append(foreignId, foreignType, encoded)
	if err != nil {
		return err
	}

	inverted.Append(int64(docId), foreignType, sforeignId)
	inverted.Append(int64(docId), typeKey, seventType)

	for i := 0; i < len(persisted.SearchKeys); i++ {
		inverted.Append(int64(docId), persisted.SearchKeys[i], persisted.SearchValues[i])
	}
	return nil
}

func ConsumeContext(envelope *spec.Context, dictionary *disk.PersistedDictionary, forward *disk.ForwardWriter) error {
	if envelope.CreatedAtNs == 0 {
		envelope.CreatedAtNs = time.Now().UnixNano()
	}

	sforeignType := depths.Cleanup(strings.ToLower(envelope.ForeignType))
	foreignType, err := dictionary.GetUniqueTerm(sforeignType)
	if err != nil {
		return err
	}

	sforeignId := depths.Cleanup(strings.ToLower(envelope.ForeignId))
	foreignId, err := dictionary.GetUniqueTerm(sforeignId)
	if err != nil {
		return err
	}

	persisted := &spec.PersistedContext{
		CreatedAtNs:  envelope.CreatedAtNs,
		PropertyKeys: []uint64{},
		ForeignType:  foreignType,
		ForeignId:    sforeignId,
	}

	for _, kv := range envelope.Properties {
		k := kv.Key
		v := kv.Value
		lc := depths.CleanupAllowDot(strings.ToLower(k))
		pk, err := dictionary.GetUniqueTerm(lc)
		if err != nil {
			return err
		}
		persisted.PropertyKeys = append(persisted.PropertyKeys, pk)
		persisted.PropertyValues = append(persisted.PropertyValues, v)
	}

	encoded, err := proto.Marshal(persisted)
	if err != nil {
		return err
	}

	_, err = forward.Append(foreignId, foreignType, encoded)
	//forward.Sync()
	if err != nil {
		return err
	}
	return nil
}
