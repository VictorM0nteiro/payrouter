// auth_test.go
// Testes do caso de uso AuthenticateClient com fakes, sem banco e sem Docker.
// Objetivo: provar que só credenciais corretas de um cliente ativo geram token, que as
// falhas (inexistente, segredo errado, inativo) são indistinguíveis para quem chama e que
// um erro de infraestrutura não é disfarçado de credencial inválida.

package app

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeClients struct {
	clients map[string]Client
	err     error
	finds   int
}

func (f *fakeClients) Find(_ context.Context, id string) (Client, error) {
	f.finds++
	if f.err != nil {
		return Client{}, f.err
	}
	c, ok := f.clients[id]
	if !ok {
		return Client{}, ErrNotFound
	}
	return c, nil
}

type fakeIssuer struct {
	issuedFor string
	err       error
}

func (f *fakeIssuer) Issue(clientID string) (string, time.Duration, error) {
	f.issuedFor = clientID
	return "token-de-" + clientID, 15 * time.Minute, f.err
}

func newAuthUC() (*AuthenticateClient, *fakeClients, *fakeIssuer) {
	clients := &fakeClients{clients: map[string]Client{
		"loja-a":   {ID: "loja-a", SecretHash: HashSecret("segredo-a"), Active: true},
		"loja-off": {ID: "loja-off", SecretHash: HashSecret("segredo-off"), Active: false},
	}}
	issuer := &fakeIssuer{}
	return NewAuthenticateClient(clients, issuer), clients, issuer
}

func TestAuthenticateClientSucesso(t *testing.T) {
	uc, _, issuer := newAuthUC()

	got, err := uc.Execute(context.Background(), "loja-a", "segredo-a")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got.AccessToken != "token-de-loja-a" || got.ExpiresIn != 15*time.Minute || issuer.issuedFor != "loja-a" {
		t.Errorf("resultado = %+v, emitido para %q", got, issuer.issuedFor)
	}
}

func TestAuthenticateClientFalhasSaoIndistinguiveis(t *testing.T) {
	tests := []struct {
		name, id, secret string
	}{
		{"cliente_inexistente", "ninguem", "segredo-a"},
		{"segredo_errado", "loja-a", "errado"},
		{"cliente_inativo_com_segredo_certo", "loja-off", "segredo-off"},
		{"id_vazio", "", "segredo-a"},
		{"segredo_vazio", "loja-a", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uc, _, issuer := newAuthUC()

			_, err := uc.Execute(context.Background(), tc.id, tc.secret)

			if !errors.Is(err, ErrInvalidCredentials) {
				t.Fatalf("erro = %v, esperado ErrInvalidCredentials", err)
			}
			if issuer.issuedFor != "" {
				t.Error("nenhum token pode ser emitido numa falha")
			}
		})
	}
}

func TestAuthenticateClientErroDoStoreNaoViraCredencialInvalida(t *testing.T) {
	uc, clients, _ := newAuthUC()
	boom := errors.New("banco fora do ar")
	clients.err = boom

	_, err := uc.Execute(context.Background(), "loja-a", "segredo-a")

	if !errors.Is(err, boom) || errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("erro = %v, esperado o do store", err)
	}
}

func TestAuthenticateClientErroDoEmissor(t *testing.T) {
	uc, _, issuer := newAuthUC()
	boom := errors.New("falha ao assinar")
	issuer.err = boom

	if _, err := uc.Execute(context.Background(), "loja-a", "segredo-a"); !errors.Is(err, boom) {
		t.Errorf("erro = %v, esperado o do emissor", err)
	}
}
