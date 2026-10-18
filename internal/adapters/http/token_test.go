// token_test.go
// Testes do endpoint POST /auth/token com casos de uso falsos, sem banco e sem Docker.
// Objetivo: provar o contrato da autenticação: 200 com o formato OAuth-like e Cache-Control
// no-store, o mesmo 401 para qualquer falha de credencial, 400 para corpo inválido, e que um
// erro de infraestrutura vira 500 sem vazar detalhe.

package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

const validCreds = `{"client_id":"clienteA","client_secret":"segredoA"}`

func TestTokenSucesso(t *testing.T) {
	h := NewHandler(&fakeAcceptor{}, &fakeGetter{}, &fakeAuthn{}, testVerifier)

	rec := do(t, h, "POST", "/auth/token", validCreds, nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, corpo %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}
	var body tokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	want := tokenResponse{AccessToken: "tok-A", TokenType: "Bearer", ExpiresIn: 900}
	if body != want {
		t.Errorf("corpo = %+v, esperado %+v", body, want)
	}
}

func TestTokenCredenciaisInvalidasSao401Identicos(t *testing.T) {
	bodies := []string{
		`{"client_id":"clienteA","client_secret":"errado"}`,
		`{"client_id":"ninguem","client_secret":"segredoA"}`,
		`{"client_id":"","client_secret":""}`,
		`{}`,
	}
	var first string
	for i, b := range bodies {
		h := NewHandler(&fakeAcceptor{}, &fakeGetter{}, &fakeAuthn{}, testVerifier)

		rec := do(t, h, "POST", "/auth/token", b, nil)

		if rec.Code != http.StatusUnauthorized || errorCode(t, rec) != "invalid_credentials" {
			t.Fatalf("%s: status %d código %q", b, rec.Code, errorCode(t, rec))
		}
		if i == 0 {
			first = rec.Body.String()
		} else if rec.Body.String() != first {
			t.Errorf("a resposta difere entre falhas:\n%s\n%s", first, rec.Body)
		}
		if rec.Header().Get("Cache-Control") == "" && strings.Contains(rec.Body.String(), "access_token") {
			t.Error("falha não pode devolver token")
		}
	}
}

func TestTokenCorpoInvalido(t *testing.T) {
	tests := []string{``, `{"client_id":`, `[]`, validCreds + `{}`, `{"client_id":"a","client_secret":"b","extra":1}`}
	for _, body := range tests {
		authn := &fakeAuthn{}
		h := NewHandler(&fakeAcceptor{}, &fakeGetter{}, authn, testVerifier)

		rec := do(t, h, "POST", "/auth/token", body, nil)

		if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "invalid_json" {
			t.Errorf("%q: status %d código %q", body, rec.Code, errorCode(t, rec))
		}
		if authn.calls != 0 {
			t.Errorf("%q: corpo inválido não pode chegar ao caso de uso", body)
		}
	}
}

func TestTokenErroInternoNaoVazaDetalhe(t *testing.T) {
	h := NewHandler(&fakeAcceptor{}, &fakeGetter{}, &fakeAuthn{err: errors.New("pq: connection refused 10.0.0.5")}, testVerifier)

	rec := do(t, h, "POST", "/auth/token", validCreds, nil)

	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Errorf("status %d, corpo %s", rec.Code, rec.Body)
	}
}

func TestTokenSoAceitaPost(t *testing.T) {
	h := NewHandler(&fakeAcceptor{}, &fakeGetter{}, &fakeAuthn{}, testVerifier)

	if rec := do(t, h, "GET", "/auth/token", "", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, esperado 405", rec.Code)
	}
}
