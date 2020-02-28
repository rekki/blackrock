package index

import (
	"errors"
	"time"

	spec "github.com/rekki/blackrock/pkg/blackrock_io"
)

var dateCache = NewDateCache()

var errMissingForeignId = errors.New("missing foreign_id")
var errMissingMetadata = errors.New("missing metadata")
var errMissingForeignType = errors.New("missing foreign_type")
var errMissingEventType = errors.New("missing event_type")

func PrepareEnvelope(envelope *spec.Envelope) error {
	meta := envelope.Metadata
	if meta == nil {
		return errMissingMetadata
	}

	if meta.CreatedAtNs == 0 {
		meta.CreatedAtNs = time.Now().UnixNano()
	}

	foreignId := meta.ForeignId
	foreignType := meta.ForeignType
	eventType := meta.EventType
	if foreignId == "" {
		return errMissingForeignId
	}
	if foreignType == "" {
		return errMissingForeignType
	}

	if eventType == "" {
		return errMissingEventType
	}

	meta.EventType = eventType
	meta.ForeignType = foreignType
	meta.ForeignId = foreignId

	for i, kv := range meta.Search {
		if kv.Value == "" {
			meta.Search[i] = spec.KV{Key: kv.Key, Value: "__empty"}
		}
	}

	// add some automatic tags
	{
		second := int32(meta.CreatedAtNs / 1e9)
		t := time.Unix(int64(second), 0).UTC()
		year, year_month, year_month_day, year_month_day_hour := dateCache.Expand(t)

		meta.Search = append(meta.Search, spec.KV{Key: "year", Value: year})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month", Value: year_month})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day", Value: year_month_day})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day-hour", Value: year_month_day_hour})
	}
	return nil
}
