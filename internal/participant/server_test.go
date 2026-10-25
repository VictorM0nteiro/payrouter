// server_test.go
// Testes do participante simulado sobre um servidor HTTP de verdade (httptest), sem Docker.
// Objetivo: provar cada um dos seis comportamentos e, sobretudo, o caso cruel: o cliente dá
// timeout, a operação já aconteceu, e a consulta de status a encontra. Também prova que a
// mesma instrução nunca é processada duas vezes e que `received` conta as chegadas, que é
// como os testes das sessões seguintes mostram que nada foi enviado em duplicidade.

package participant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fastConfig usa atrasos curtos para os testes não esperarem segundos.
func fastConfig() Config {
	return Config{SlowDelay: 150 * time.Millisecond, LateDelay: 400 * time.Millisecond}
}

func newServer(t *testing.T, cfg Config) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(New(cfg))
	t.Cleanup(srv.Close)
	return srv
}

func body(id, e2e string) []byte {
	b, _ := json.Marshal(Request{InstructionID: id, AmountCents: 15000, Debtor: "d", Creditor: "c", EndToEndID: e2e})
	return b
}

// pay envia a instrução com um prazo de cliente. Devolve o status HTTP e o corpo decodificado.
func pay(t *testing.T, srv *httptest.Server, timeout time.Duration, payload []byte) (int, map[string]any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/payments", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("corpo inválido: %v", err)
	}
	return resp.StatusCode, out, nil
}

// status consulta GET /payments/{id}.
func status(t *testing.T, srv *httptest.Server, id string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(srv.URL + "/payments/" + id) //nolint:noctx // teste
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("corpo inválido: %v", err)
	}
	return resp.StatusCode, out
}

func TestAcceptResponde200AcceptedNaHora(t *testing.T) {
	srv := newServer(t, fastConfig())
	start := time.Now()

	code, out, err := pay(t, srv, 2*time.Second, body("i1", "e2e-1"))

	if err != nil || code != http.StatusOK || out["status"] != "accepted" {
		t.Fatalf("resposta = %d %v, %v", code, out, err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Errorf("accept demorou %v, esperado imediato", time.Since(start))
	}
}

func TestRejectResponderComOCodigoConfigurado(t *testing.T) {
	cfg := fastConfig()
	cfg.RejectCode = "account_closed"
	srv := newServer(t, cfg)

	code, out, err := pay(t, srv, 2*time.Second, body("i2", "reject:1"))

	if err != nil || code != http.StatusOK || out["status"] != "rejected" || out["code"] != "account_closed" {
		t.Fatalf("resposta = %d %v, %v", code, out, err)
	}
	if _, st := status(t, srv, "i2"); st["status"] != "rejected" {
		t.Errorf("status = %v, esperado rejected", st)
	}
}

func TestRejectUsaOCodigoPadrao(t *testing.T) {
	srv := newServer(t, fastConfig())

	_, out, _ := pay(t, srv, 2*time.Second, body("i3", "reject:1"))

	if out["code"] != "insufficient_funds" {
		t.Errorf("code = %v, esperado insufficient_funds", out["code"])
	}
}

func TestSlowRespondeDepoisDoAtrasoMasDentroDoPrazo(t *testing.T) {
	cfg := fastConfig()
	srv := newServer(t, cfg)
	start := time.Now()

	code, out, err := pay(t, srv, 3*time.Second, body("i4", "slow:1"))

	if err != nil || code != http.StatusOK || out["status"] != "accepted" {
		t.Fatalf("resposta = %d %v, %v", code, out, err)
	}
	if elapsed := time.Since(start); elapsed < cfg.SlowDelay {
		t.Errorf("slow respondeu em %v, antes do atraso de %v", elapsed, cfg.SlowDelay)
	}
}

func TestDropNaoProcessaNemResponde(t *testing.T) {
	srv := newServer(t, fastConfig())

	_, _, err := pay(t, srv, 300*time.Millisecond, body("i5", "drop:1"))

	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("erro = %v, esperado o timeout do cliente", err)
	}
	// A mensagem se perdeu: a consulta de status não acha nada, mas ela chegou uma vez.
	code, st := status(t, srv, "i5")
	if code != http.StatusNotFound || st["status"] != "not_found" || st["received"] != float64(1) {
		t.Errorf("status = %d %v; esperado 404 not_found received=1", code, st)
	}
}

func TestLateEOCasoCruelOClienteDaTimeoutMasAOperacaoAconteceu(t *testing.T) {
	cfg := fastConfig()
	srv := newServer(t, cfg)

	// O prazo do cliente (150ms) é menor que o atraso da resposta (400ms): timeout.
	_, _, err := pay(t, srv, 150*time.Millisecond, body("i6", "late:1"))
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("erro = %v, esperado o timeout do cliente", err)
	}

	// O cliente não sabe de nada, mas a consulta de status mostra que foi processada.
	code, st := status(t, srv, "i6")
	if code != http.StatusOK || st["status"] != "accepted" || st["received"] != float64(1) {
		t.Fatalf("status = %d %v; esperado 200 accepted received=1", code, st)
	}
}

func TestCrashChamaExitAntesDeProcessar(t *testing.T) {
	var exitCode atomic.Int32
	cfg := fastConfig()
	cfg.Exit = func(code int) { exitCode.Store(int32(code)) }
	srv := newServer(t, cfg)

	// Com Exit injetado o handler retorna sem escrever corpo; o que importa é o estado.
	resp, err := http.Post(srv.URL+"/payments", "application/json", bytes.NewReader(body("i7", "crash:1"))) //nolint:noctx // teste
	if err == nil {
		_ = resp.Body.Close()
	}

	if exitCode.Load() != 1 {
		t.Errorf("Exit chamado com %d, esperado 1", exitCode.Load())
	}
	code, st := status(t, srv, "i7")
	if code != http.StatusNotFound {
		t.Errorf("status = %d %v; a instrução não pode ter sido processada", code, st)
	}
}

func TestMesmaInstrucaoDuasVezesEProcessadaUmaVezSo(t *testing.T) {
	srv := newServer(t, fastConfig())

	_, first, _ := pay(t, srv, 2*time.Second, body("i8", "reject:1"))
	// O reenvio chega com outro marcador, que mandaria aceitar: o resultado guardado vale.
	_, second, _ := pay(t, srv, 2*time.Second, body("i8", "accept:1"))

	if first["status"] != "rejected" || second["status"] != "rejected" {
		t.Errorf("respostas = %v e %v; esperado o mesmo resultado rejected", first, second)
	}
	if _, st := status(t, srv, "i8"); st["received"] != float64(2) {
		t.Errorf("received = %v, esperado 2", st["received"])
	}
}

func TestPadraoDoProcessoVaiParaInstrucoesSemMarcador(t *testing.T) {
	cfg := fastConfig()
	cfg.Default = Reject
	srv := newServer(t, cfg)

	_, out, _ := pay(t, srv, 2*time.Second, body("i9", "e2e-sem-marcador"))

	if out["status"] != "rejected" {
		t.Errorf("status = %v, esperado rejected (o padrão do processo)", out["status"])
	}
}

func TestConsultaDeInstrucaoNuncaVistaE404ComReceivedZero(t *testing.T) {
	srv := newServer(t, fastConfig())

	code, st := status(t, srv, "nunca-vista")

	if code != http.StatusNotFound || st["received"] != float64(0) {
		t.Errorf("status = %d %v", code, st)
	}
}

func TestPedidosInvalidosViram400ComOFormatoDeErro(t *testing.T) {
	srv := newServer(t, fastConfig())
	tests := []struct {
		name    string
		payload string
	}{
		{"json_quebrado", `{"instruction_id":`},
		{"sem_instruction_id", `{"amount_cents":100,"debtor":"d","creditor":"c","end_to_end_id":"e"}`},
		{"sem_end_to_end_id", `{"instruction_id":"x","amount_cents":100,"debtor":"d","creditor":"c"}`},
		{"sem_debtor", `{"instruction_id":"x","amount_cents":100,"creditor":"c","end_to_end_id":"e"}`},
		{"valor_zero", `{"instruction_id":"x","amount_cents":0,"debtor":"d","creditor":"c","end_to_end_id":"e"}`},
		{"valor_negativo", `{"instruction_id":"x","amount_cents":-5,"debtor":"d","creditor":"c","end_to_end_id":"e"}`},
		{"valor_decimal", `{"instruction_id":"x","amount_cents":10.5,"debtor":"d","creditor":"c","end_to_end_id":"e"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, out, err := pay(t, srv, 2*time.Second, []byte(tc.payload))
			if err != nil || code != http.StatusBadRequest {
				t.Fatalf("resposta = %d, %v; esperado 400", code, err)
			}
			if e, ok := out["error"].(map[string]any); !ok || e["code"] == "" || e["message"] == "" {
				t.Errorf("corpo = %v, esperado {\"error\":{\"code\",\"message\"}}", out)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	srv := newServer(t, fastConfig())

	resp, err := http.Get(srv.URL + "/healthz") //nolint:noctx // teste
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, esperado 200", resp.StatusCode)
	}
}

func TestMetodoErradoNaoEPermitido(t *testing.T) {
	srv := newServer(t, fastConfig())

	resp, err := http.Get(srv.URL + "/payments") //nolint:noctx // teste
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /payments = %d, esperado 405", resp.StatusCode)
	}
}

func TestCemInstrucoesEmParaleloSaoTodasGravadas(t *testing.T) {
	srv := newServer(t, fastConfig())
	const n = 100

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, out, err := pay(t, srv, 5*time.Second, body(fmt.Sprintf("p%d", i), fmt.Sprintf("e2e-%d", i)))
			if err != nil || code != http.StatusOK || out["status"] != "accepted" {
				t.Errorf("p%d: %d %v %v", i, code, out, err)
			}
		}()
	}
	wg.Wait()

	for i := range n {
		if code, st := status(t, srv, fmt.Sprintf("p%d", i)); code != http.StatusOK || st["received"] != float64(1) {
			t.Fatalf("p%d: status %d %v", i, code, st)
		}
	}
}
