package spec

import (
	"errors"
)

func ValidateEnvelope(envelope *Envelope) error {
	if envelope.Metadata == nil {
		return errors.New("need metadata key")
	}
	if envelope.Metadata.ForeignId == "" {
		return errors.New("need foreign_id in metadata")
	}

	if envelope.Metadata.ForeignType == "" {
		return errors.New("need foreign_type in metadata")
	}

	if envelope.Metadata.EventType == "" {
		return errors.New("need event_type in metadata")

	}
	return nil
}

func ValidateContext(ctx *Context) error {
	if ctx.ForeignId == "" {
		return errors.New("need foreign_id")
	}

	if ctx.ForeignType == "" {
		return errors.New("need foreign_type")
	}
	return nil
}
