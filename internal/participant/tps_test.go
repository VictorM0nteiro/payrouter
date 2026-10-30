// tps_test.go
// Testes do limite de taxa real (TPS) do participante simulado, sobre HTTP de verdade.
// Objetivo: provar que acima do TPS o participante responde 429 e NÃO processa nada (a consulta de
// status não acha a instrução, `throttled` conta o 429 e `received` não conta), e que depois que o
// balde recupera a mesma instrução é admitida e processada uma única vez. Sem TPS, nada é limitado.

package participant

import (
	"net/http"
	"testing"
	"time"
)

func TestAcimaDoTPSDa429SemProcessarENadaSeGrava(t *testing.T) {
	cfg := fastConfig()
	cfg.TPS, cfg.Burst = 2, 2
	srv := newServer(t, cfg)

	codes := make([]int, 0, 4)
	for _, id := range []string{"a", "b", "c", "d"} {
		code, _, err := pay(t, srv, 2*time.Second, body(id, "accept:"+id))
		if err != nil {
			t.Fatal(err)
		}
		codes = append(codes, code)
	}

	// Rajada de 2 passa; o resto leva 429 (nenhum segundo se passou para repor fichas).
	if codes[0] != http.StatusOK || codes[1] != http.StatusOK || codes[2] != http.StatusTooManyRequests || codes[3] != http.StatusTooManyRequests {
		t.Fatalf("códigos = %v, esperado [200 200 429 429]", codes)
	}
	scode, st := status(t, srv, "c")
	if scode != http.StatusNotFound || st["received"] != float64(0) || st["throttled"] != float64(1) {
		t.Errorf("status de c = %d %v; esperado 404 received=0 throttled=1 (recusada, não processada)", scode, st)
	}
}

func TestDepoisDeRecuperarOBaldeAInstrucaoRecusadaEAdmitidaUmaVezSo(t *testing.T) {
	cfg := fastConfig()
	cfg.TPS, cfg.Burst = 20, 1 // uma ficha a cada 50ms
	srv := newServer(t, cfg)
	if code, _, err := pay(t, srv, 2*time.Second, body("x", "accept:x")); err != nil || code != http.StatusOK {
		t.Fatalf("x = %d, %v; esperado 200", code, err)
	}
	if code, _, _ := pay(t, srv, 2*time.Second, body("y", "accept:y")); code != http.StatusTooManyRequests {
		t.Fatalf("y = %d, esperado 429", code)
	}
	time.Sleep(120 * time.Millisecond)

	code, out, err := pay(t, srv, 2*time.Second, body("y", "accept:y"))

	if err != nil || code != http.StatusOK || out["status"] != "accepted" {
		t.Fatalf("y reenviada = %d %v %v; esperado 200 accepted", code, out, err)
	}
	_, st := status(t, srv, "y")
	if st["received"] != float64(1) || st["throttled"] != float64(1) {
		t.Errorf("status de y = %v; esperado received=1 throttled=1", st)
	}
}

func TestSemTPSNadaELimitado(t *testing.T) {
	srv := newServer(t, fastConfig())

	for i := range 20 {
		id := string(rune('a' + i))
		if code, _, err := pay(t, srv, 2*time.Second, body(id, "accept:"+id)); err != nil || code != http.StatusOK {
			t.Fatalf("%s = %d, %v; esperado 200 sem limite", id, code, err)
		}
	}
}
