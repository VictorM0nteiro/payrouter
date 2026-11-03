// mongotest.go
// Helper de testes que sobe um MongoDB 7 em container.
// Objetivo: dar aos testes da projeção um Mongo real com uma chamada, como o pgtest faz com o
// Postgres e o kafkatest com o Redpanda. Só deve ser importado por testes.

package mongotest

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
)

// Start sobe o container e devolve a URL de conexão e uma função que o encerra. Se algo falhar
// no meio, o que já subiu é encerrado antes de devolver o erro.
func Start(ctx context.Context) (string, func(), error) {
	ctr, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr)
		}
		return "", nil, fmt.Errorf("subindo o Mongo de teste (o Docker está rodando?): %w", err)
	}
	stop := func() { _ = testcontainers.TerminateContainer(ctr) }
	url, err := ctr.ConnectionString(ctx)
	if err != nil {
		stop()
		return "", nil, err
	}
	return url, stop, nil
}
