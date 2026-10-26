// consumer.go
// Adapter Kafka que entrega ao caso de uso as mensagens de um tópico, sobre franz-go, em
// grupo de consumidores.
// Objetivo: at-least-once sem nunca descartar. O commit do offset é manual e só acontece
// depois que o handler terminou; erro transitório repete a MESMA mensagem com backoff, no
// lugar, sem pular para a próxima (preserva a ordem por partição e aplica contrapressão). A
// única exceção é app.ErrOutcomeUnknown, que é uma resposta definitiva do handler: o envio
// já foi feito e reenviar é perigoso, então o offset avança. Mensagem sem os headers
// esperados (veneno) é registrada e pulada, porque repeti-la não a conserta.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	minRetryBackoff = 200 * time.Millisecond
	maxRetryBackoff = 10 * time.Second

	// handleTimeout é o prazo de cada tentativa do handler (envio + gravações).
	handleTimeout = 30 * time.Second
)

// Handler trata uma mensagem. Ver app.ProcessInstruction.Handle para o contrato do erro.
type Handler func(ctx context.Context, msg app.Message) error

// Consumer lê um tópico em grupo.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer entra no grupo para ler o tópico. Sem offset salvo, começa do início, para
// nada que já esteja no tópico ser perdido.
func NewConsumer(brokers []string, group, topic string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("criando consumidor Kafka: %w", err)
	}
	return &Consumer{client: client}, nil
}

// Run consome até o contexto terminar. A mensagem em andamento é concluída antes de sair.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if ctx.Err() == nil {
				slog.ErrorContext(ctx, "consumer: erro no fetch", "topic", topic, "partition", partition, "error", err)
			}
		})

		var done []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if ctx.Err() != nil {
				return
			}
			if c.process(ctx, handle, r) {
				done = append(done, r)
			}
		})
		if len(done) == 0 {
			continue
		}
		// Os registros concluídos entram no commit mesmo se o contexto terminou no meio.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if err := c.client.CommitRecords(commitCtx, done...); err != nil {
			slog.ErrorContext(ctx, "consumer: commit falhou, as mensagens serão repetidas", "error", err)
		}
		cancel()
	}
	return nil
}

// process devolve true se o registro foi concluído (seu offset pode ser confirmado) e false
// apenas se o contexto terminou antes de concluí-lo.
func (c *Consumer) process(ctx context.Context, handle Handler, r *kgo.Record) bool {
	msg, err := toMessage(r)
	if err != nil {
		slog.ErrorContext(ctx, "consumer: mensagem inválida descartada", "partition", r.Partition, "offset", r.Offset, "error", err)
		return true
	}

	backoff := minRetryBackoff
	for {
		// O handler termina o que começou mesmo durante o desligamento: depois do envio,
		// abandonar no meio deixaria a instrução sem resposta gravada. O prazo impede que um
		// banco travado prenda o consumidor para sempre; o estouro é erro transitório.
		hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handleTimeout)
		err := handle(hctx, msg)
		cancel()
		switch {
		case err == nil:
			return true
		case errors.Is(err, app.ErrOutcomeUnknown):
			slog.WarnContext(ctx, "consumer: resultado desconhecido, não será reenviado", "instruction_id", msg.AggregateID, "error", err)
			return true
		}
		slog.ErrorContext(ctx, "consumer: erro transitório, repetindo a mensagem", "instruction_id", msg.AggregateID,
			"error", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxRetryBackoff)
	}
}

// toMessage extrai chave e headers; o corpo não é lido.
func toMessage(r *kgo.Record) (app.Message, error) {
	msg := app.Message{AggregateID: string(r.Key)}
	if msg.AggregateID == "" {
		return app.Message{}, errors.New("registro sem chave")
	}
	for _, h := range r.Headers {
		switch h.Key {
		case "event_type":
			msg.EventType = string(h.Value)
		case "version":
			v, err := strconv.Atoi(string(h.Value))
			if err != nil || v <= 0 {
				return app.Message{}, fmt.Errorf("header version inválido: %q", h.Value)
			}
			msg.Version = v
		}
	}
	if msg.EventType == "" || msg.Version == 0 {
		return app.Message{}, errors.New("registro sem os headers event_type e version")
	}
	return msg, nil
}

// Close sai do grupo e encerra o cliente.
func (c *Consumer) Close() {
	c.client.Close()
}
