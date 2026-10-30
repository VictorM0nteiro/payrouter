// throttle_test.go
// Testes do cliente HTTP do participante diante de um 429.
// Objetivo: provar a distinção mais importante do projeto depois do unknown. 429 é "definitivamente
// não processado" (*app.ThrottledError, que NÃO é ErrOutcomeUnknown, com o Retry-After lido),
// enquanto 500 e timeout continuam sendo resultado desconhecido. Mesma falha aparente, reações
// opostas.

package participant

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

func TestPay429ENaoProcessadoComRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, time.Second).Pay(context.Background(), request("i1", "e2e"))

	var throttled *app.ThrottledError
	if !errors.As(err, &throttled) || throttled.RetryAfter != 2*time.Second {
		t.Fatalf("erro = %v, esperado *ThrottledError com Retry-After de 2s", err)
	}
	if !errors.Is(err, app.ErrThrottled) || errors.Is(err, app.ErrOutcomeUnknown) {
		t.Errorf("429 deve ser ErrThrottled e NUNCA ErrOutcomeUnknown: %v", err)
	}
}

func TestPay429SemRetryAfterVale0(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "amanhã")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, time.Second).Pay(context.Background(), request("i2", "e2e"))

	var throttled *app.ThrottledError
	if !errors.As(err, &throttled) || throttled.RetryAfter != 0 {
		t.Fatalf("erro = %v, esperado *ThrottledError com Retry-After 0", err)
	}
}

func TestPay5xxContinuaSendoResultadoDesconhecidoEnquanto429NaoE(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, time.Second).Pay(context.Background(), request("i3", "e2e"))

	if !errors.Is(err, app.ErrOutcomeUnknown) || errors.Is(err, app.ErrThrottled) {
		t.Errorf("erro = %v, esperado ErrOutcomeUnknown e não ErrThrottled", err)
	}
}

func TestStatus429ContinuaSendoDesconhecido(t *testing.T) {
	// Uma consulta recusada por taxa não diz nada sobre a instrução: nunca vira "não processada".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, time.Second).Status(context.Background(), "i4")

	if !errors.Is(err, app.ErrOutcomeUnknown) || errors.Is(err, app.ErrThrottled) {
		t.Errorf("erro = %v, esperado ErrOutcomeUnknown", err)
	}
}
