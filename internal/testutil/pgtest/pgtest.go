// pgtest.go
// Helper de testes que sobe um PostgreSQL 16 em container e aplica a migration inicial.
// Objetivo: dar a qualquer pacote de teste um banco real com uma chamada, sem repetir a
// configuração do testcontainers. O pacote postgres mantém o seu TestMain próprio; este
// helper serve aos pacotes novos (como o da API HTTP). Só deve ser importado por testes.

package pgtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Start sobe o container, aplica a migration e devolve o pool e uma função que encerra tudo.
// Se algo falhar no meio, o que já subiu é encerrado antes de devolver o erro.
func Start(ctx context.Context) (*pgxpool.Pool, func(), error) {
	ctr, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("payrouter"),
		tcpostgres.WithUsername("payrouter"),
		tcpostgres.WithPassword("payrouter"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr)
		}
		return nil, nil, fmt.Errorf("subindo o Postgres de teste (o Docker está rodando?): %w", err)
	}

	var pool *pgxpool.Pool
	stop := func() {
		if pool != nil {
			pool.Close()
		}
		_ = testcontainers.TerminateContainer(ctr)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		stop()
		return nil, nil, err
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		stop()
		return nil, nil, err
	}
	cfg.MaxConns = 32
	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		stop()
		return nil, nil, err
	}
	if err := applyMigration(ctx, pool); err != nil {
		stop()
		return nil, nil, err
	}
	return pool, stop, nil
}

// applyMigration roda todos os *.up.sql em ordem de nome, pela conexão crua, que aceita
// várias instruções por arquivo.
func applyMigration(ctx context.Context, pool *pgxpool.Pool) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	files, err := filepath.Glob(filepath.Join(root, "migrations", "*.up.sql"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := conn.Conn().PgConn().Exec(ctx, string(sql)).ReadAll(); err != nil {
			return fmt.Errorf("aplicando %s: %w", filepath.Base(f), err)
		}
	}
	return nil
}

// repoRoot sobe a partir do diretório atual até achar o go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod não encontrado acima de %s", dir)
		}
		dir = parent
	}
}
