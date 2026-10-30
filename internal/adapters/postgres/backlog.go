// backlog.go
// Leitura das filas do banco para as métricas: tarefas de conciliação pendentes e o outbox ainda
// não publicado (quantidade e idade da linha mais antiga).
// Objetivo: alimentar os gauges do Prometheus numa única consulta, sem escrever nada.

package postgres

import (
	"context"
	"fmt"

	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const selectBacklog = `
SELECT (SELECT count(*) FROM reconciliation_tasks),
       (SELECT count(*) FROM outbox WHERE published_at IS NULL),
       COALESCE((SELECT extract(epoch FROM now() - min(created_at)) FROM outbox WHERE published_at IS NULL), 0)::float8`

// BacklogReader lê as filas do banco.
type BacklogReader struct {
	pool *pgxpool.Pool
}

func NewBacklogReader(pool *pgxpool.Pool) *BacklogReader {
	return &BacklogReader{pool: pool}
}

// Read devolve as filas neste instante. Serve de argumento para metrics.RegisterBacklog.
func (b *BacklogReader) Read(ctx context.Context) (metrics.Backlog, error) {
	var out metrics.Backlog
	if err := b.pool.QueryRow(ctx, selectBacklog).Scan(&out.ReconciliationTasks, &out.OutboxPending, &out.OutboxOldestAge); err != nil {
		return metrics.Backlog{}, fmt.Errorf("lendo as filas do banco: %w", err)
	}
	return out, nil
}
