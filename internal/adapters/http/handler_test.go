// handler_test.go
// Testes dos handlers HTTP com casos de uso falsos, sem banco e sem Docker.
// Objetivo: provar o contrato da API: autenticação, leitura estrita do corpo (inclusive valor
// não inteiro), headers de resposta, que o scope vem do cliente autenticado e a tabela inteira
// de erro para status, sem nunca vazar a mensagem de um erro interno.

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

const (
	validUUID = "7c9e1f0a-3b2d-4c5e-8f6a-1b2c3d4e5f60"
	validBody = `{"debtor":"d","creditor":"c","amount_cents":15000,"end_to_end_id":"e2e"}`
)

type fakeAcceptor struct {
	got    app.AcceptInstructionInput
	result app.AcceptInstructionResult
	err    error
	called bool
}

func (f *fakeAcceptor) Execute(_ context.Context, in app.AcceptInstructionInput) (app.AcceptInstructionResult, error) {
	f.called, f.got = true, in
	return f.result, f.err
}

type fakeGetter struct {
	view app.InstructionView
	err  error
	got  domain.InstructionID
}

func (f *fakeGetter) Execute(_ context.Context, id domain.InstructionID) (app.InstructionView, error) {
	f.got = id
	return f.view, f.err
}

// fakeVerifier conhece dois tokens: tok-A (clienteA) e tok-B (clienteB).
type fakeVerifier struct{}

func (fakeVerifier) Verify(token string) (string, error) {
	switch token {
	case "tok-A":
		return "clienteA", nil
	case "tok-B":
		return "clienteB", nil
	}
	return "", app.ErrInvalidToken
}

// fakeAuthn aceita só clienteA/segredoA e conta as chamadas.
type fakeAuthn struct {
	err   error
	calls int
}

func (f *fakeAuthn) Execute(_ context.Context, id, secret string) (app.AuthToken, error) {
	f.calls++
	if f.err != nil {
		return app.AuthToken{}, f.err
	}
	if id != "clienteA" || secret != "segredoA" {
		return app.AuthToken{}, app.ErrInvalidCredentials
	}
	return app.AuthToken{AccessToken: "tok-A", ExpiresIn: 15 * time.Minute}, nil
}

var (
	testVerifier = fakeVerifier{}
	testAuthn    = &fakeAuthn{}
)

func do(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func authed(extra map[string]string) map[string]string {
	h := map[string]string{"Authorization": "Bearer tok-A", "Idempotency-Key": "k1"}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("corpo de erro inválido %q: %v", rec.Body.String(), err)
	}
	return body.Error.Code
}

func TestPostAutenticacao(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"sem_header_e_401", map[string]string{"Idempotency-Key": "k1"}, http.StatusUnauthorized},
		{"token_invalido_e_401", map[string]string{"Authorization": "Bearer errado", "Idempotency-Key": "k1"}, http.StatusUnauthorized},
		{"token_vazio_e_401", map[string]string{"Authorization": "Bearer ", "Idempotency-Key": "k1"}, http.StatusUnauthorized},
		{"esquema_basic_e_401", map[string]string{"Authorization": "Basic tok-A", "Idempotency-Key": "k1"}, http.StatusUnauthorized},
		{"sem_esquema_e_401", map[string]string{"Authorization": "tok-A", "Idempotency-Key": "k1"}, http.StatusUnauthorized},
		{"x_api_key_nao_vale_mais", map[string]string{"X-API-Key": "segredoA", "Idempotency-Key": "k1"}, http.StatusUnauthorized},
		{"token_valido_passa", authed(nil), http.StatusAccepted},
		{"esquema_em_minusculas_passa", map[string]string{"Authorization": "bearer tok-A", "Idempotency-Key": "k1"}, http.StatusAccepted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acc := &fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted}}
			h := NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier)

			rec := do(t, h, "POST", "/instructions", validBody, tc.headers)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, esperado %d (corpo %s)", rec.Code, tc.want, rec.Body)
			}
			if tc.want == http.StatusUnauthorized {
				if acc.called {
					t.Error("sem autenticação o caso de uso não pode ser chamado")
				}
				if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
					t.Errorf("WWW-Authenticate = %q", got)
				}
				if errorCode(t, rec) != "unauthorized" {
					t.Errorf("código = %q", errorCode(t, rec))
				}
			}
		})
	}
}

func TestPostSucessoDevolve202ComLocation(t *testing.T) {
	acc := &fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted}}
	h := NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier)

	rec := do(t, h, "POST", "/instructions", validBody, authed(nil))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, corpo %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Location"); got != "/instructions/"+validUUID {
		t.Errorf("Location = %q", got)
	}
	if rec.Header().Get("Idempotent-Replayed") != "" {
		t.Error("requisição nova não deve ter Idempotent-Replayed")
	}
	var body acceptResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.ID != validUUID || body.Status != "accepted" {
		t.Errorf("corpo = %s (erro %v)", rec.Body, err)
	}
}

func TestPostRepeticaoMarcaOHeader(t *testing.T) {
	acc := &fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted, Replayed: true}}
	h := NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier)

	rec := do(t, h, "POST", "/instructions", validBody, authed(nil))

	if rec.Code != http.StatusAccepted || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("status %d, Idempotent-Replayed %q", rec.Code, rec.Header().Get("Idempotent-Replayed"))
	}
}

func TestPostRepassaOsDadosAoCasoDeUso(t *testing.T) {
	acc := &fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted}}
	h := NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier)

	do(t, h, "POST", "/instructions", validBody, authed(map[string]string{"Authorization": "Bearer tok-B", "Idempotency-Key": "chave-9"}))

	want := app.AcceptInstructionInput{
		Scope: "clienteB", IdempotencyKey: "chave-9",
		Debtor: "d", Creditor: "c", AmountCents: 15000, EndToEndID: "e2e",
	}
	if acc.got != want {
		t.Errorf("input = %+v, esperado %+v", acc.got, want)
	}
}

func TestPostCorpoInvalido(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"corpo_vazio", ``},
		{"json_quebrado", `{"debtor":`},
		{"nao_e_objeto", `[1,2,3]`},
		{"campo_desconhecido", `{"debtor":"d","creditor":"c","amount_cents":1,"end_to_end_id":"e","extra":true}`},
		{"lixo_depois_do_json", validBody + ` {"x":1}`},
		{"valor_decimal_e_recusado", `{"debtor":"d","creditor":"c","amount_cents":15000.5,"end_to_end_id":"e"}`},
		{"valor_em_notacao_cientifica", `{"debtor":"d","creditor":"c","amount_cents":1.5e4,"end_to_end_id":"e"}`},
		{"valor_como_texto", `{"debtor":"d","creditor":"c","amount_cents":"15000","end_to_end_id":"e"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acc := &fakeAcceptor{}
			h := NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier)

			rec := do(t, h, "POST", "/instructions", tc.body, authed(nil))

			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "invalid_json" {
				t.Errorf("status %d, código %q", rec.Code, errorCode(t, rec))
			}
			if acc.called {
				t.Error("corpo inválido não pode chegar ao caso de uso")
			}
		})
	}
}

func TestPostCorpoGrandeDemais(t *testing.T) {
	acc := &fakeAcceptor{}
	h := NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier)
	big := `{"debtor":"` + strings.Repeat("x", maxBodyBytes+1) + `"}`

	rec := do(t, h, "POST", "/instructions", big, authed(nil))

	if rec.Code != http.StatusRequestEntityTooLarge || errorCode(t, rec) != "body_too_large" {
		t.Errorf("status %d, código %q", rec.Code, errorCode(t, rec))
	}
}

func TestTabelaDeErros(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"idempotency_key_required", app.ErrIdempotencyKeyRequired, 400, "idempotency_key_required"},
		{"invalid_amount", domain.ErrInvalidAmount, 422, "invalid_amount"},
		{"invalid_parties", domain.ErrInvalidParties, 422, "invalid_parties"},
		{"idempotency_key_reuse", app.ErrIdempotencyKeyReuse, 422, "idempotency_key_reuse"},
		{"not_found", app.ErrNotFound, 404, "not_found"},
		{"concurrency_conflict", app.ErrConcurrencyConflict, 409, "concurrency_conflict"},
		{"erro_embrulhado_ainda_e_reconhecido", errors.Join(errors.New("contexto"), domain.ErrInvalidAmount), 422, "invalid_amount"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(&fakeAcceptor{err: tc.err}, &fakeGetter{}, testAuthn, testVerifier)

			rec := do(t, h, "POST", "/instructions", validBody, authed(nil))

			if rec.Code != tc.wantStatus || errorCode(t, rec) != tc.wantCode {
				t.Errorf("status %d código %q, esperado %d %q", rec.Code, errorCode(t, rec), tc.wantStatus, tc.wantCode)
			}
		})
	}
}

func TestErroInternoNaoVazaDetalhe(t *testing.T) {
	secret := "pq: password authentication failed for user payrouter"
	h := NewHandler(&fakeAcceptor{err: errors.New(secret)}, &fakeGetter{}, testAuthn, testVerifier)

	rec := do(t, h, "POST", "/instructions", validBody, authed(nil))

	if rec.Code != http.StatusInternalServerError || errorCode(t, rec) != "internal" {
		t.Fatalf("status %d código %q", rec.Code, errorCode(t, rec))
	}
	if strings.Contains(rec.Body.String(), "password") || strings.Contains(rec.Body.String(), "payrouter") {
		t.Errorf("a resposta vazou o erro interno: %s", rec.Body)
	}
}

func TestGetInstrucao(t *testing.T) {
	view := app.InstructionView{
		ID: validUUID, Status: domain.StatusUnknown, Version: 3, AmountCents: 15000,
		Debtor: "d", Creditor: "c", EndToEndID: "e2e", Attempts: 1,
	}
	getter := &fakeGetter{view: view}
	h := NewHandler(&fakeAcceptor{}, getter, testAuthn, testVerifier)

	rec := do(t, h, "GET", "/instructions/"+validUUID, "", map[string]string{"Authorization": "Bearer tok-A"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, corpo %s", rec.Code, rec.Body)
	}
	var got instructionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := instructionResponse{
		ID: validUUID, Status: "unknown", Version: 3, AmountCents: 15000,
		Debtor: "d", Creditor: "c", EndToEndID: "e2e", Attempts: 1,
	}
	if got != want {
		t.Errorf("corpo = %+v, esperado %+v", got, want)
	}
	if getter.got != validUUID {
		t.Errorf("id repassado = %q", getter.got)
	}
}

func TestGetSemAutenticacaoE401(t *testing.T) {
	h := NewHandler(&fakeAcceptor{}, &fakeGetter{}, testAuthn, testVerifier)

	rec := do(t, h, "GET", "/instructions/"+validUUID, "", nil)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d", rec.Code)
	}
}

func TestGetIdInvalido(t *testing.T) {
	tests := []string{
		"abc",
		"123",
		"7c9e1f0a3b2d4c5e8f6a1b2c3d4e5f60",       // UUID sem hífens
		"{7c9e1f0a-3b2d-4c5e-8f6a-1b2c3d4e5f60}", // com chaves
		"urn:uuid:7c9e1f0a-3b2d-4c5e-8f6a-1b2c3d4e5f60", // com prefixo urn
		"7c9e1f0a-3b2d-4c5e-8f6a-1b2c3d4e5f6z",          // caractere inválido
	}
	for _, id := range tests {
		t.Run(id, func(t *testing.T) {
			getter := &fakeGetter{}
			h := NewHandler(&fakeAcceptor{}, getter, testAuthn, testVerifier)

			rec := do(t, h, "GET", "/instructions/"+id, "", map[string]string{"Authorization": "Bearer tok-A"})

			if rec.Code != http.StatusBadRequest || errorCode(t, rec) != "invalid_id" {
				t.Errorf("status %d código %q", rec.Code, errorCode(t, rec))
			}
			if getter.got != "" {
				t.Error("id inválido não pode chegar ao caso de uso")
			}
		})
	}
}

func TestGetInexistenteE404(t *testing.T) {
	h := NewHandler(&fakeAcceptor{}, &fakeGetter{err: app.ErrNotFound}, testAuthn, testVerifier)

	rec := do(t, h, "GET", "/instructions/"+validUUID, "", map[string]string{"Authorization": "Bearer tok-A"})

	if rec.Code != http.StatusNotFound || errorCode(t, rec) != "not_found" {
		t.Errorf("status %d código %q", rec.Code, errorCode(t, rec))
	}
}

func TestMetodoNaoPermitido(t *testing.T) {
	h := NewHandler(&fakeAcceptor{}, &fakeGetter{}, testAuthn, testVerifier)

	rec := do(t, h, "DELETE", "/instructions/"+validUUID, "", map[string]string{"Authorization": "Bearer tok-A"})

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, esperado 405", rec.Code)
	}
}
