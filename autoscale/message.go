package autoscale

// Message is the payload published by the producer and consumed by consumers.
type Message struct {
	Key       string `json:"key"`       // partition key (e.g. account ID)
	Sequence  int    `json:"sequence"`  // per-key monotonic sequence
	Payload   string `json:"payload"`
	Partition int    `json:"partition"` // which partition this was routed to (set by router)
	Timestamp int64  `json:"ts"`
}

// ConsumerResult tracks what a single consumer instance received.
type ConsumerResult struct {
	Partition int
	Received  int
	Messages  []Message
}

// AccountSeq tracks the last consumed sequence for an account.
type AccountSeq struct {
	Key       string `json:"key"`
	Sequence  int    `json:"sequence"`
	Partition int    `json:"partition"`
	Gaps      int    `json:"gaps"`      // number of skipped sequences detected
	GapTotal  int    `json:"gap_total"` // total missing messages across all gaps
}
