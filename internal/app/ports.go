// ports.go
// Portas (interfaces) da camada de aplicação e os erros que atravessam a fronteira.
// Objetivo: o caso de uso conversa com o event store por esta interface, sem saber que
// existe Postgres. O adapter implementa; o domínio e os casos de uso continuam testáveis
// com um fake em memória.

package app

import (
	"context"
	"errors"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

var (
	// ErrConcurrencyConflict: a versão esperada não bate com a do event store, porque
	// outro comando gravou antes. O chamador relê o agregado e tenta de novo.
	ErrConcurrencyConflict = errors.New("conflito de concorrência: versão esperada desatualizada")

	// ErrNotFound: não existe nenhum evento para o agregado pedido.
	ErrNotFound = errors.New("instrução não encontrada")
)

// EventStore guarda e devolve o histórico de eventos de uma instrução.
type EventStore interface {
	// Append grava events em sequência, a partir de expectedVersion+1, numa única
	// transação: ou todos entram, ou nenhum. Devolve ErrConcurrencyConflict se a versão
	// atual do agregado não for exatamente expectedVersion.
	Append(ctx context.Context, id domain.InstructionID, expectedVersion int, events ...domain.Event) error

	// Load devolve o histórico em ordem de versão, ou ErrNotFound se estiver vazio.
	Load(ctx context.Context, id domain.InstructionID) ([]domain.Event, error)
}
