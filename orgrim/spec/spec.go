package spec

type EventMetadata struct {
	Partition   int32    `json:"partition"`
	Offset      int64    `json:"offset"`
	Topic       string   `json:"topic"`
	Tags        []string `json:"tags"`
	CreatedAtNs int64    `json:"created_at_ns"`
	RemoteAddr  string   `json:"remote_addr"`
}
