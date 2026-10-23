// producer.go
// Adapter Kafka da porta app.Publisher, sobre franz-go.
// Objetivo: publicar o lote do outbox com a chave igual ao aggregate_id (mesma partição,
// portanto em ordem) e só devolver sucesso quando o broker confirmou TODAS as mensagens.
// O produtor é idempotente (padrão do franz-go): retries internos não duplicam nem
// reordenam. Duplicatas só nascem da queda do relay entre publicar e marcar, e essas são
// absorvidas pelo consumidor, idempotente pela versão.

package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/twmb/franz-go/pkg/kgo"
)

// deliveryTimeout é o prazo total para o broker confirmar um registro, retries incluídos.
const deliveryTimeout = 30 * time.Second

// producer implementa app.Publisher
type Producer struct {
	client *kgo.Client
}

var _ app.Publisher = (*Producer)(nil)

// NewProducer conecta aos brokers (lista de host:porta).
func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("criando cliente Kafka: %w", err)
	}
	return &Producer{client: client}, nil
}

// Publish envio o lote e espera a confirmação de todos os registros
func (p *Producer) Publish(ctx context.Context, msgs []app.OutboxMessage) error {
	records := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		records[i] = &kgo.Record{
			Topic: m.Topic,
			Key:   []byte(m.AggregateID),
			Value: m.Payload,
			Headers: []kgo.RecordHeader{
				{Key: "event_type", Value: []byte(m.EventType)},
				{Key: "version", Value: []byte(strconv.Itoa(m.Version))},
			},
		}
	}
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produzindo no kafka: %w", err)
	}
	return nil
}

// Close encerra o cliente, esperando o que ainda estiver em voo.
func (p *Producer) Close() {
	p.client.Close()
}
