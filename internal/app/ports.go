// ports.go
// Portas (interfaces) da camada de aplicação e os erros que atravessam a fronteira.
// Objetivo: o caso de uso conversa com o banco só por estas interfaces, sem saber que existe
// Postgres. O adapter implementa; o domínio e os casos de uso continuam testáveis. A
// UnitOfWork é o que permite gravar evento e chave de idempotência na MESMA transação.

package app

import (
	"context"
	"errors"
	"time"

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
	// Append grava events em sequência, a partir de expectedVersion+1, de forma atômica:
	// ou todos entram, ou nenhum. Devolve ErrConcurrencyConflict se a versão atual do
	// agregado não for exatamente expectedVersion.
	Append(ctx context.Context, id domain.InstructionID, expectedVersion int, events ...domain.Event) error

	// Load devolve o histórico em ordem de versão, ou ErrNotFound se estiver vazio.
	Load(ctx context.Context, id domain.InstructionID) ([]domain.Event, error)
}

// IdempotencyKey identifica uma requisição repetível. O escopo (quem pediu) faz parte da
// chave: a mesma Idempotency-Key de dois clientes são duas chaves diferentes.
type IdempotencyKey struct {
	Scope    string
	Endpoint string
	Key      string
}

// IdempotencyRecord é o que ficou gravado para uma chave já usada.
type IdempotencyRecord struct {
	Fingerprint  string
	AggregateID  domain.InstructionID
	ResponseBody []byte
}

// IdempotencyStore registra as chaves de idempotência dentro de uma transação.
type IdempotencyStore interface {
	// Claim tenta registrar a chave. Se ela era nova, devolve claimed=true e a chave passa
	// a pertencer à transação atual. Se já existia, devolve claimed=false e o registro
	// gravado, para o chamador comparar o fingerprint e repetir a resposta.
	Claim(ctx context.Context, key IdempotencyKey, fingerprint string) (record IdempotencyRecord, claimed bool, err error)

	// Complete guarda a resposta da chave que esta transação reivindicou.
	Complete(ctx context.Context, key IdempotencyKey, aggregateID domain.InstructionID, responseBody []byte) error
}

// ReconciliationStore guarda a fila de trabalho da conciliação: uma tarefa por instrução em
// unknown. É gravada na MESMA transação que o evento RoutingTimedOut, então não existe unknown
// sem tarefa nem tarefa sem unknown.
type ReconciliationStore interface {
	// Schedule cria a tarefa da instrução para a primeira consulta em at. Idempotente: se a
	// tarefa já existe, nada muda.
	Schedule(ctx context.Context, id domain.InstructionID, at time.Time) error
}

// Tx é o conjunto de stores ligados a uma mesma transação de banco.
type Tx interface {
	Events() EventStore
	Idempotency() IdempotencyStore
	Reconciliation() ReconciliationStore
}

// UnitOfWork executa fn dentro de uma única transação. Se fn devolver erro, tudo é
// desfeito; se devolver nil, tudo é confirmado junto. É por aqui que evento, idempotência
// e (na sessão 9) outbox ficam atômicos entre si.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}
