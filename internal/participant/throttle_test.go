// throttle_test.go
// Testes do comportamento throttle do participante simulado (HTTP de verdade, sem Docker).
// Objetivo: provar o 429 como "não processado". A primeira chegada leva 429 com Retry-After e
// NADA é gravado (a consulta de status não acha a instrução); a segunda é processada como um
// accept. O contador `throttled` conta os 429 e `received` só conta as chegadas admitidas, então
// received == 1 continua sendo a prova de que a instrução foi enviada uma única vez.

package participant

import (
	"bytes"
	"net/http"
	"testing"
	"time"
)

func TestThrottleDa429NaPrimeiraChegadaSemProcessarNada(t *testing.T) {
	srv := newServer(t, fastConfig())

	code, out, err := pay(t, srv, 2*time.Second, body("t1", "throttle:1"))

	if err != nil || code != http.StatusTooManyRequests {
		t.Fatalf("primeira chegada = %d, %v; esperado 429", code, err)
	}
	if e, _ := out["error"].(map[string]any); e["code"] != "rate_limited" {
		t.Errorf("corpo do 429 = %v, esperado error.code rate_limited", out)
	}
	scode, st := status(t, srv, "t1")
	if scode != http.StatusNotFound || st["received"] != float64(0) || st["throttled"] != float64(1) {
		t.Errorf("status = %d %v; esperado 404 received=0 throttled=1 (nada foi processado)", scode, st)
	}
}

func TestThrottleProcessaNaSegundaChegada(t *testing.T) {
	srv := newServer(t, fastConfig())
	if code, _, _ := pay(t, srv, 2*time.Second, body("t2", "throttle:1")); code != http.StatusTooManyRequests {
		t.Fatalf("primeira chegada = %d, esperado 429", code)
	}

	code, out, err := pay(t, srv, 2*time.Second, body("t2", "throttle:1"))

	if err != nil || code != http.StatusOK || out["status"] != "accepted" {
		t.Fatalf("segunda chegada = %d %v %v; esperado 200 accepted", code, out, err)
	}
	_, st := status(t, srv, "t2")
	if st["received"] != float64(1) || st["throttled"] != float64(1) {
		t.Errorf("status = %v; esperado received=1 throttled=1", st)
	}
}

func TestThrottleManda429ComRetryAfter(t *testing.T) {
	srv := newServer(t, fastConfig())

	resp, err := http.Post(srv.URL+"/payments", "application/json", bytes.NewReader(body("t3", "throttle:1"))) //nolint:noctx // teste
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "1" {
		t.Errorf("status=%d Retry-After=%q; esperado 429 e 1", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}
