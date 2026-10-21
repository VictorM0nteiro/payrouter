package app

// OutboxMessage é uma linha pendente do outbox, pronta para publicar.
type OutboxMessage struct {
	ID          int64
	AggregateID string
	Topic       string
	EventType   string
	Version     int
	Payload     []byte
}


