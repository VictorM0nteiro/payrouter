package app

import (
	"context"
	"log/slog"
	"time"
)

// OutboxMessage é uma linha pendente do outbox, pronta para publicar.
type OutboxMessage struct {
	ID          int64
	AggregateID string
	Topic       string
	EventType   string
	Version     int
	Payload     []byte
}

// OutboxStore entrega ao relay as mensagens pendentes.
type OutboxStore interface {
	// Drain reivindica até limit mensagens pendentes (no máximo a mais antiga de cada
	// agregado, para vários relays não reordenarem um agregado), chama publish com elas e,
	// só se publish devolver nil, marca todas como publicadas. Se publish falhar, nada é
	// marcado e o erro é devolvido. Devolve quantas mensagens foram publicadas e marcadas.
	Drain(ctx context.Context, limit int, publish func(ctx context.Context, msgs []OutboxMessage) error) (int, error)
}

// publisher envia as mensagens ao broker
type Publisher interface {
	// Publish só devolve nil quando o broker confirmou TODAS as mensagens do lote.
	Publish(ctx context.Context, msg []OutboxMessage) error
}

// mas relay backoff limita a espera crescente entre tentativas após um erro
const maxRelayBackoff = 10 * time.Second

// relay publica o outbox no broker
type Relay struct {
	store        OutboxStore
	publisher    Publisher
	batchSize    int
	pollInterval time.Duration
}

func NewRelay(store OutboxStore, publisher Publisher, batchSize int, pollInterval time.Duration) *Relay {
	return &Relay{store: store, publisher: publisher, batchSize: batchSize, pollInterval: pollInterval}
}

// run once faz uma rodada publica um lote e o marca. Devolve quantas msgs saíram
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	return r.store.Drain(ctx, r.batchSize, r.publisher.Publish)
}

// Run repete as rodadas até o contexto terminar. Havendo mensagens, a proxima rodada começa na hora
// (um agregado só avança um evento por rodade); vazio espera pollInterval; erro,
// espera com backoff e tenta de novo, sem nunca descartar nada
func (r *Relay) Run(ctx context.Context) error {
	backoff := r.pollInterval
	for ctx.Err() == nil {
		n, err := r.RunOnce(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			slog.ErrorContext(ctx, "relay: rodada falhou, será repetida", "error", err, "retry_in", backoff.String())
			sleep(ctx, backoff)
			backoff = min(backoff*2, maxRelayBackoff)
		case n == 0:
			backoff = r.pollInterval
			sleep(ctx, r.pollInterval)
		default:
			backoff = r.pollInterval
		}
	}
	return nil
}

//sleep espera d ou até o conteúdo terminar
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}