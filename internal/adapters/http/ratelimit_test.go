// ratelimit_test.go
// Testes do limite de entrada por cliente, sobre o handler com fakes (sem banco, sem Docker).
// Objetivo: provar que a rajada passa e o excedente leva 429 rate_limited com Retry-After sem
// executar o caso de uso, que cada cliente tem o seu balde (um não consome a cota do outro), que
// o limite recupera com o tempo e que, sem configuração, nada é limitado.

package httpapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

func postAs(t *testing.T, h http.Handler, token, key string) int {
	t.Helper()
	rec := do(t, h, "POST", "/instructions", validBody, map[string]string{"Authorization": "Bearer " + token, "Idempotency-Key": key})
	return rec.Code
}

// countingAcceptor conta quantas vezes o caso de uso foi executado.
type countingAcceptor struct {
	fakeAcceptor
	calls int
}

func (c *countingAcceptor) Execute(ctx context.Context, in app.AcceptInstructionInput) (app.AcceptInstructionResult, error) {
	c.calls++
	return c.fakeAcceptor.Execute(ctx, in)
}

func newLimited(rps float64, burst int) (http.Handler, *countingAcceptor) {
	acc := &countingAcceptor{fakeAcceptor: fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted}}}
	return NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier, WithRateLimit(rps, burst)), acc
}

func TestExcedenteLevaOs429ComRetryAfterSemExecutarOCasoDeUso(t *testing.T) {
	h, acc := newLimited(1, 2)

	got := []int{postAs(t, h, "tok-A", "k1"), postAs(t, h, "tok-A", "k2"), postAs(t, h, "tok-A", "k3")}

	if got[0] != http.StatusAccepted || got[1] != http.StatusAccepted || got[2] != http.StatusTooManyRequests {
		t.Fatalf("códigos = %v, esperado [202 202 429]", got)
	}
	rec := do(t, h, "POST", "/instructions", validBody, authed(map[string]string{"Idempotency-Key": "k4"}))
	if rec.Code != http.StatusTooManyRequests || errorCode(t, rec) != "rate_limited" || rec.Header().Get("Retry-After") == "" {
		t.Errorf("429 = %d %s Retry-After=%q; esperado 429 rate_limited com Retry-After", rec.Code, rec.Body, rec.Header().Get("Retry-After"))
	}
	if acc.calls != 2 {
		t.Errorf("casos de uso executados = %d, esperado 2: o excedente não pode chegar ao caso de uso", acc.calls)
	}
}

func TestCadaClienteTemOSeuBalde(t *testing.T) {
	h, _ := newLimited(1, 1)

	a1 := postAs(t, h, "tok-A", "k1")
	a2 := postAs(t, h, "tok-A", "k2")
	b1 := postAs(t, h, "tok-B", "k1")

	if a1 != http.StatusAccepted || a2 != http.StatusTooManyRequests || b1 != http.StatusAccepted {
		t.Errorf("A=%d,%d B=%d; esperado A=202,429 e B=202 (B não paga pelo excesso de A)", a1, a2, b1)
	}
}

func TestOLimiteRecuperaComOTempo(t *testing.T) {
	h, _ := newLimited(50, 1) // uma ficha a cada 20ms
	if postAs(t, h, "tok-A", "k1") != http.StatusAccepted {
		t.Fatal("a primeira deveria passar")
	}
	if postAs(t, h, "tok-A", "k2") != http.StatusTooManyRequests {
		t.Fatal("a segunda, imediata, deveria levar 429")
	}
	time.Sleep(60 * time.Millisecond)

	if got := postAs(t, h, "tok-A", "k3"); got != http.StatusAccepted {
		t.Errorf("depois de esperar = %d, esperado 202", got)
	}
}

func TestSemConfiguracaoNadaELimitado(t *testing.T) {
	h := NewHandler(&fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted}},
		&fakeGetter{}, testAuthn, testVerifier)

	for i := range 50 {
		if got := postAs(t, h, "tok-A", string(rune('a'+i%26))+string(rune('a'+i/26))); got != http.StatusAccepted {
			t.Fatalf("requisição %d = %d, esperado 202 sem limite", i, got)
		}
	}
}
