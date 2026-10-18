// clientstore_test.go
// Testes do ClientStore contra um Postgres real.
// Objetivo: provar o ciclo de vida do cliente: cadastrar, achar com o hash gravado, recusar id
// duplicado, desativar e tratar o inexistente como ErrNotFound. Cada teste usa um id novo.

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

func newClientStore(t *testing.T) (*ClientStore, string) {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	return NewClientStore(testPool), "cliente-" + string(newID(t))
}

func TestClientStoreCriaEAcha(t *testing.T) {
	store, id := newClientStore(t)
	ctx := context.Background()
	hash := app.HashSecret("segredo")

	if err := store.Create(ctx, id, hash); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := store.Find(ctx, id)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if got.ID != id || !got.Active || string(got.SecretHash) != string(hash) {
		t.Errorf("cliente = %+v", got)
	}
}

func TestClientStoreIdDuplicado(t *testing.T) {
	store, id := newClientStore(t)
	ctx := context.Background()
	if err := store.Create(ctx, id, app.HashSecret("a")); err != nil {
		t.Fatal(err)
	}

	if err := store.Create(ctx, id, app.HashSecret("b")); !errors.Is(err, app.ErrClientExists) {
		t.Errorf("erro = %v, esperado ErrClientExists", err)
	}
}

func TestClientStoreInexistenteEhNotFound(t *testing.T) {
	store, id := newClientStore(t)

	if _, err := store.Find(context.Background(), id); !errors.Is(err, app.ErrNotFound) {
		t.Errorf("find: erro = %v, esperado ErrNotFound", err)
	}
	if err := store.Deactivate(context.Background(), id); !errors.Is(err, app.ErrNotFound) {
		t.Errorf("deactivate: erro = %v, esperado ErrNotFound", err)
	}
}

func TestClientStoreDesativa(t *testing.T) {
	store, id := newClientStore(t)
	ctx := context.Background()
	if err := store.Create(ctx, id, app.HashSecret("a")); err != nil {
		t.Fatal(err)
	}

	if err := store.Deactivate(ctx, id); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	got, err := store.Find(ctx, id)
	if err != nil || got.Active {
		t.Errorf("depois de desativar: %+v, %v", got, err)
	}
}
