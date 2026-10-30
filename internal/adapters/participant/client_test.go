// client_test.go
// Testes do cliente HTTP do participante contra o participante simulado de verdade (httptest).
// Objetivo: provar a distinção que a tese exige. accepted e rejected são conclusivos; timeout
// (drop e late), conexão caída, 5xx e corpo ilegível são ErrOutcomeUnknown. No caso late o
// cliente desiste mas o participante processou: a consulta de status mostra a operação, e o
// participante recebeu a instrução uma única vez.

package participant

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	sim "github.com/VictorM0nteiro/payrouter/internal/participant"
)

func request(id, e2e string) app.PaymentRequest {
	return app.PaymentRequest{InstructionID: id, AmountCents: 15000, Debtor: "d", Creditor: "c", EndToEndID: e2e}
}

func newSim(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(sim.New(sim.Config{SlowDelay: 100 * time.Millisecond, LateDelay: 600 * time.Millisecond}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAcceptedERejectedSaoConclusivos(t *testing.T) {
	srv := newSim(t)
	c := NewClient(srv.URL, 2*time.Second)

	got, err := c.Pay(context.Background(), request("a1", "accept:1"))
	if err != nil || got.Status != app.PaymentAccepted {
		t.Errorf("accept = %+v, %v", got, err)
	}
	got, err = c.Pay(context.Background(), request("r1", "reject:1"))
	if err != nil || got.Status != app.PaymentRejected || got.Code != "insufficient_funds" {
		t.Errorf("reject = %+v, %v", got, err)
	}
	got, err = c.Pay(context.Background(), request("s1", "slow:1"))
	if err != nil || got.Status != app.PaymentAccepted {
		t.Errorf("slow dentro do prazo = %+v, %v", got, err)
	}
}

func TestDropELateViramResultadoDesconhecido(t *testing.T) {
	srv := newSim(t)
	c := NewClient(srv.URL, 250*time.Millisecond)

	for _, id := range []string{"drop", "late"} {
		_, err := c.Pay(context.Background(), request(id+"-1", id+":1"))
		if !errors.Is(err, app.ErrOutcomeUnknown) {
			t.Errorf("%s: erro = %v, esperado ErrOutcomeUnknown", id, err)
		}
	}

	// drop: a instrução chegou e se perdeu. late: chegou e foi processada. Uma vez cada.
	if code, st := status(t, srv, "drop-1"); code != http.StatusNotFound || st["received"] != float64(1) {
		t.Errorf("drop: %d %v; esperado 404 received=1", code, st)
	}
	if code, st := status(t, srv, "late-1"); code != http.StatusOK || st["status"] != "accepted" || st["received"] != float64(1) {
		t.Errorf("late: %d %v; esperado 200 accepted received=1", code, st)
	}
}

func TestConexaoCaidaE5xxECorpoIlegivelViramResultadoDesconhecido(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"http_500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }},
		{"corpo_ilegivel", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("não é json")) }},
		{"status_desconhecido", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"talvez"}`)) }},
		{"rejeicao_sem_codigo", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"rejected"}`)) }},
		{"conexao_cortada", func(w http.ResponseWriter, _ *http.Request) {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()

			_, err := NewClient(srv.URL, time.Second).Pay(context.Background(), request("x", "e2e"))

			if !errors.Is(err, app.ErrOutcomeUnknown) {
				t.Errorf("erro = %v, esperado ErrOutcomeUnknown", err)
			}
		})
	}
}

func TestServidorForaDoArEResultadoDesconhecido(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	_, err := NewClient(url, time.Second).Pay(context.Background(), request("x", "e2e"))

	if !errors.Is(err, app.ErrOutcomeUnknown) {
		t.Errorf("erro = %v, esperado ErrOutcomeUnknown", err)
	}
}

func TestCorpoEnviadoUsaCentavosInteiros(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}))
	defer srv.Close()

	if _, err := NewClient(srv.URL+"/", time.Second).Pay(context.Background(), request("i1", "e2e-1")); err != nil {
		t.Fatal(err)
	}

	if got["instruction_id"] != "i1" || got["amount_cents"] != float64(15000) || got["end_to_end_id"] != "e2e-1" ||
		got["debtor"] != "d" || got["creditor"] != "c" {
		t.Errorf("corpo = %v", got)
	}
}

func status(t *testing.T, srv *httptest.Server, id string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/payments/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

func TestStatusAceitaRejeitadaENaoEncontrada(t *testing.T) {
	srv := newSim(t)
	c := NewClient(srv.URL, 2*time.Second)
	for _, id := range []string{"sa", "sr"} {
		e2e := map[string]string{"sa": "accept:1", "sr": "reject:1"}[id]
		if _, err := c.Pay(context.Background(), request(id, e2e)); err != nil {
			t.Fatal(err)
		}
	}

	got, err := c.Status(context.Background(), "sa")
	if err != nil || got.Status != app.PaymentAccepted {
		t.Errorf("accepted = %+v, %v", got, err)
	}
	got, err = c.Status(context.Background(), "sr")
	if err != nil || got.Status != app.PaymentRejected || got.Code != "insufficient_funds" {
		t.Errorf("rejected = %+v, %v", got, err)
	}
	// 404 é "sem registro": uma resposta, não um erro, e nunca prova que nada foi processado.
	got, err = c.Status(context.Background(), "nunca-vista")
	if err != nil || got.Status != app.PaymentNotFound {
		t.Errorf("not_found = %+v, %v", got, err)
	}
}

func TestStatusDoLateMostraQueFoiProcessadaDepoisDoTimeout(t *testing.T) {
	srv := newSim(t)
	c := NewClient(srv.URL, 250*time.Millisecond)
	if _, err := c.Pay(context.Background(), request("lt", "late:1")); !errors.Is(err, app.ErrOutcomeUnknown) {
		t.Fatalf("Pay = %v, esperado ErrOutcomeUnknown", err)
	}

	got, err := c.Status(context.Background(), "lt")

	if err != nil || got.Status != app.PaymentAccepted {
		t.Errorf("Status = %+v, %v; esperado accepted: o participante processou apesar do timeout", got, err)
	}
}

func TestStatusComFalhaViraResultadoDesconhecido(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"http_500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }},
		{"corpo_ilegivel", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("não é json")) }},
		{"status_desconhecido", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"talvez"}`)) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()

			_, err := NewClient(srv.URL, time.Second).Status(context.Background(), "x")

			if !errors.Is(err, app.ErrOutcomeUnknown) {
				t.Errorf("erro = %v, esperado ErrOutcomeUnknown", err)
			}
		})
	}
}
