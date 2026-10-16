// eventstore_test.go
// Testes de integração do EventStore contra um Postgres 16 real (testcontainers).
// Objetivo: provar o contrato do append com versão esperada, a atomicidade, a ausência de
// buracos no histórico e a invariante de versões contíguas. Exigem Docker rodando; sem
// ele, falham com mensagem clara em vez de passar em silêncio. O teste de N goroutines
// concorrentes é da sessão 4.

package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

var (
	testPool      *pgxpool.Pool
	testContainer testcontainers.Container
	setupErr      error
)

// TestMain sobe um único container para o pacote inteiro. Os testes se isolam usando um
// aggregate_id novo cada um, então não precisam limpar tabelas.
func TestMain(m *testing.M) {
	ctx := context.Background()
	setupErr = startPostgres(ctx)

	code := m.Run()

	if testPool != nil {
		testPool.Close()
	}
	if testContainer != nil {
		_ = testContainer.Terminate(ctx)
	}
	os.Exit(code)
}

func startPostgres(ctx context.Context) error {
	ctr, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("payrouter"),
		tcpostgres.WithUsername("payrouter"),
		tcpostgres.WithPassword("payrouter"),
		tcpostgres.BasicWaitStrategies(),
	)
	if ctr != nil {
		testContainer = ctr
	}
	if err != nil {
		return err
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	return applyMigration(ctx, testPool)
}

// applyMigration roda o 000001_init.up.sql. Usa o protocolo simples da conexão crua, que
// aceita várias instruções numa só chamada.
func applyMigration(ctx context.Context, pool *pgxpool.Pool) error {
	sql, err := os.ReadFile("../../../migrations/000001_init.up.sql")
	if err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Conn().PgConn().Exec(ctx, string(sql)).ReadAll()
	return err
}

// newStore devolve o store e falha o teste, com instrução, se o Docker não subiu.
func newStore(t *testing.T) *EventStore {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	return NewEventStore(testPool)
}

// newID gera um UUID novo usando o próprio Postgres, sem dependência extra.
func newID(t *testing.T) domain.InstructionID {
	t.Helper()
	var id string
	if err := testPool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		t.Fatalf("gerando uuid: %v", err)
	}
	return domain.InstructionID(id)
}

func ctxWithTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func accepted(id domain.InstructionID) domain.InstructionAccepted {
	return domain.InstructionAccepted{ID: id, Amount: 15000, Debtor: "d", Creditor: "c", EndToEndID: "e2e"}
}

func TestAppendELoadDevolvemHistoricoEmOrdem(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatalf("append v1: %v", err)
	}
	if err := store.Append(ctx, id, 1, domain.RoutingAttempted{}, domain.RoutingTimedOut{}); err != nil {
		t.Fatalf("append v2-v3: %v", err)
	}

	events, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p, err := domain.Rehydrate(events)
	if err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	if p.Status() != domain.StatusUnknown || p.Version() != 3 || p.Attempts() != 1 {
		t.Errorf("agregado = %s/v%d/%d tentativas, esperado unknown/v3/1", p.Status(), p.Version(), p.Attempts())
	}
	if p.ID() != id || p.Amount().Cents() != 15000 {
		t.Errorf("dados do accepted perdidos: id=%s valor=%d", p.ID(), p.Amount().Cents())
	}
}

func TestAppendComVersaoEsperadaDesatualizadaEConflito(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatalf("append v1: %v", err)
	}
	// Outro comando que também leu "versão 0" tenta gravar a versão 1 de novo.
	err := store.Append(ctx, id, 0, domain.RoutingAttempted{})
	if !errors.Is(err, app.ErrConcurrencyConflict) {
		t.Fatalf("erro = %v, esperado ErrConcurrencyConflict", err)
	}

	events, err := store.Load(ctx, id)
	if err != nil || len(events) != 1 {
		t.Fatalf("o conflito não deveria ter gravado nada: %d eventos, erro %v", len(events), err)
	}
}

func TestAppendComVersaoEsperadaAFrenteNaoDeixaBuraco(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatalf("append v1: %v", err)
	}
	// O agregado está na versão 1; afirmar 5 abriria um buraco (v1, depois v6).
	err := store.Append(ctx, id, 5, domain.RoutingAttempted{})
	if !errors.Is(err, app.ErrConcurrencyConflict) {
		t.Fatalf("erro = %v, esperado ErrConcurrencyConflict", err)
	}
}

func TestAppendDeVariosEventosEAtomico(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	// O segundo evento não é codificável; o primeiro, já inserido na transação, não pode
	// sobreviver.
	err := store.Append(ctx, id, 0, accepted(id), eventoDesconhecido{})
	if err == nil {
		t.Fatal("esperava erro de codificação")
	}
	if _, err := store.Load(ctx, id); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("a transação deveria ter sido desfeita por inteiro, load = %v", err)
	}
}

func TestAppendSemEventosNaoFazNada(t *testing.T) {
	store := newStore(t)
	if err := store.Append(ctxWithTimeout(t), newID(t), 0); err != nil {
		t.Fatalf("append vazio deveria ser no-op: %v", err)
	}
}

func TestLoadDeAgregadoInexistenteEhNotFound(t *testing.T) {
	store := newStore(t)
	if _, err := store.Load(ctxWithTimeout(t), newID(t)); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("erro = %v, esperado ErrNotFound", err)
	}
}

func TestInvarianteDeVersoesContiguas(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, id, 1, domain.RoutingAttempted{}); err != nil {
		t.Fatal(err)
	}
	// Tentativas ilegais que, se passassem, quebrariam a invariante.
	_ = store.Append(ctx, id, 0, domain.RoutingAttempted{})
	_ = store.Append(ctx, id, 9, domain.RoutingAttempted{})

	sql, err := os.ReadFile("testdata/invariants.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Só a invariante 1 vale por enquanto. A 2 (todo evento tem uma linha de outbox)
	// passa a valer na sessão 9, quando o outbox é gravado na mesma transação.
	invariante1 := strings.Split(string(sql), ";")[0]

	rows, err := testPool.Query(ctx, invariante1)
	if err != nil {
		t.Fatalf("executando invariante 1: %v", err)
	}
	defer rows.Close()
	var violadores []string
	for rows.Next() {
		var agg string
		if err := rows.Scan(&agg); err != nil {
			t.Fatal(err)
		}
		violadores = append(violadores, agg)
	}
	if len(violadores) != 0 {
		t.Fatalf("agregados com versões não contíguas: %v", violadores)
	}
}
