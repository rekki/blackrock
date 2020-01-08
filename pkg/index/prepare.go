package index

import (
	"errors"
	"strings"
	"time"

	spec "github.com/rekki/blackrock/pkg/blackrock_io"
	"github.com/rekki/blackrock/pkg/depths"
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

	foreignId := depths.Cleanup(strings.ToLower(meta.ForeignId))
	foreignType := depths.Cleanup(strings.ToLower(meta.ForeignType))
	eventType := depths.Cleanup(strings.ToLower(meta.EventType))
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
	second := int32(meta.CreatedAtNs / 1e9)
	for i, kv := range meta.Search {
		k := kv.Key
		v := kv.Value

		lc := depths.Cleanup(strings.ToLower(k))

		if lc == "event_type" || lc == "foreign_type" || lc == "foreign_id" || lc == foreignType || lc == "" {
			continue
		}

		value := depths.Cleanup(strings.ToLower(v))
		if value == "" {
			value = "__empty"
		}
		meta.Search[i] = spec.KV{Key: lc, Value: value}
	}

	// add some automatic tags
	{
		t := time.Unix(int64(second), 0).UTC()
		year, year_month, year_month_day, year_month_day_hour := dateCache.Expand(t)

		meta.Search = append(meta.Search, spec.KV{Key: "year", Value: year})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month", Value: year_month})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day", Value: year_month_day})
		meta.Search = append(meta.Search, spec.KV{Key: "year-month-day-hour", Value: year_month_day_hour})
	}
	return nil
}
