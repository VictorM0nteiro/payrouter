// kafkatest.go
// Helper de testes que sobe um Redpanda (compatível com Kafka) em container, cria tópicos e
// lê tudo o que foi publicado.
// Objetivo: dar aos testes de integração um broker real com uma chamada, como o pgtest faz
// com o Postgres. Só deve ser importado por testes.

package kafkatest

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Start sobe o broker e devolve a lista de brokers e a função que o encerra.
func Start(ctx context.Context) ([]string, func(), error) {
	ctr, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v24.3.18")
	if err != nil {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr)
		}
		return nil, nil, fmt.Errorf("subindo o Redpanda de teste (o Docker está rodando?): %w", err)
	}
	stop := func() { _ = testcontainers.TerminateContainer(ctr) }
	broker, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		stop()
		return nil, nil, err
	}
	return []string{broker}, stop, nil
}

// CreateTopic cria o tópico com o número de partições pedido.
func CreateTopic(ctx context.Context, brokers []string, topic string, partitions int32) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	_, err = kadm.NewClient(client).CreateTopic(ctx, partitions, 1, nil, topic)
	return err
}

// Record é o que o consumidor de teste vê.
type Record struct {
	Key       string
	Value     []byte
	Partition int32
	Headers   map[string]string
}

// ReadAll lê o tópico do início até o fim atual e devolve os registros na ordem de leitura
// (ordenados dentro de cada partição). Falha se não terminar em timeout.
func ReadAll(ctx context.Context, brokers []string, topic string, timeout time.Duration) ([]Record, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	ends, err := kadm.NewClient(admin).ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("listando offsets finais: %w", err)
	}
	var want int64
	ends.Each(func(o kadm.ListedOffset) { want += o.Offset })

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	var out []Record
	for int64(len(out)) < want {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("lidos %d de %d registros: %w", len(out), want, err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			h := make(map[string]string, len(r.Headers))
			for _, hd := range r.Headers {
				h[hd.Key] = string(hd.Value)
			}
			out = append(out, Record{Key: string(r.Key), Value: r.Value, Partition: r.Partition, Headers: h})
		})
	}
	return out, nil
}
