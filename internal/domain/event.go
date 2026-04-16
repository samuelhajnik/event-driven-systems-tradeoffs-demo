package domain

// Event is the ingestion unit flowing through API, Kafka, and sink.
type Event struct {
	EventID     string `json:"event_id"`
	DeviceID    string `json:"device_id"`
	Timestamp   int64  `json:"timestamp"`
	EventType   string `json:"event_type"`
	Payload     string `json:"payload"`
	FailureMode string `json:"failure_mode,omitempty"`
	RetryCount  int    `json:"retry_count,omitempty"`
	SourceTopic string `json:"source_topic,omitempty"`
}
