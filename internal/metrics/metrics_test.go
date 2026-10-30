// metrics_test.go
// Testes das métricas, sem banco e sem Docker.
// Objetivo: provar que 429 e timeout caem em séries DIFERENTES, que a espera do limite de saída
// é somada, que rodadas vazias do relay não entram, que um receptor nulo não faz nada, que a
// consulta das filas é lida na hora da coleta (e omite as séries se falhar) e que o servidor
// responde GET /metrics e para quando o contexto termina.

package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// value lê uma série do registry pelo nome e pelos labels; ausente vale 0.
func value(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !hasLabels(m.GetLabel(), labels) {
				continue
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return 0
}

func hasLabels(got []*dto.LabelPair, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, l := range got {
			if l.GetName() == k && l.GetValue() == v {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestA429EOTimeoutCaemEmSeriesDiferentes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewProcessor(reg)

	m.Call("pay", "throttled", 10*time.Millisecond)
	m.Call("pay", "unknown", 3*time.Second)
	m.Call("pay", "unknown", 3*time.Second)

	if got := value(t, reg, "payrouter_participant_calls_total", map[string]string{"op": "pay", "result": "throttled"}); got != 1 {
		t.Errorf("pay throttled = %v, esperado 1", got)
	}
	if got := value(t, reg, "payrouter_participant_calls_total", map[string]string{"op": "pay", "result": "unknown"}); got != 2 {
		t.Errorf("pay unknown = %v, esperado 2", got)
	}
	if got := value(t, reg, "payrouter_participant_call_duration_seconds", map[string]string{"op": "pay"}); got != 3 {
		t.Errorf("amostras de duração = %v, esperado 3", got)
	}
}

func TestContrapressaoSomaAEspera(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewProcessor(reg)

	m.Backpressure(250 * time.Millisecond)
	m.Backpressure(750 * time.Millisecond)

	if got := value(t, reg, "payrouter_output_backpressure_total", nil); got != 2 {
		t.Errorf("esperas = %v, esperado 2", got)
	}
	if got := value(t, reg, "payrouter_output_backpressure_wait_seconds_total", nil); got != 1 {
		t.Errorf("soma da espera = %v s, esperado 1", got)
	}
}

func TestRodadasVaziasDoRelayNaoEntramMasErroEntra(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewRelay(reg)

	m.Round(0, nil, time.Millisecond)             // vazia: ignorada
	m.Round(5, nil, 20*time.Millisecond)          // publicou 5
	m.Round(0, errors.New("broker"), time.Second) // falhou

	if got := value(t, reg, "payrouter_relay_published_total", nil); got != 5 {
		t.Errorf("publicadas = %v, esperado 5", got)
	}
	if got := value(t, reg, "payrouter_relay_round_errors_total", nil); got != 1 {
		t.Errorf("erros = %v, esperado 1", got)
	}
	if got := value(t, reg, "payrouter_relay_round_duration_seconds", nil); got != 2 {
		t.Errorf("rodadas medidas = %v, esperado 2 (a vazia não conta)", got)
	}
}

func TestReceptorNuloNaoFazNada(t *testing.T) {
	var p *Processor
	var r *Relay

	p.Call("pay", "ok", time.Second)
	p.Outcome("settled")
	p.Backpressure(time.Second)
	p.Reconciled("resolved")
	p.Record("ok")
	p.Pause()
	r.Round(1, nil, time.Second)
}

func TestFilasSaoLidasNaColetaEOmitidasSeAConsultaFalha(t *testing.T) {
	reg := prometheus.NewRegistry()
	fail := false
	RegisterBacklog(reg, func(context.Context) (Backlog, error) {
		if fail {
			return Backlog{}, errors.New("banco fora")
		}
		return Backlog{ReconciliationTasks: 3, OutboxPending: 7, OutboxOldestAge: 12.5}, nil
	})

	if got := value(t, reg, "payrouter_reconciliation_tasks_pending", nil); got != 3 {
		t.Errorf("tarefas = %v, esperado 3", got)
	}
	if got := value(t, reg, "payrouter_outbox_pending", nil); got != 7 {
		t.Errorf("outbox pendente = %v, esperado 7", got)
	}
	if got := value(t, reg, "payrouter_outbox_oldest_pending_age_seconds", nil); got != 12.5 {
		t.Errorf("idade = %v, esperado 12.5", got)
	}

	fail = true
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "payrouter_") {
			t.Errorf("série %s presente com o banco fora, esperado omitida (não mentir com zero)", f.GetName())
		}
	}
}

func TestRequisicoesHTTPContamPorRotaEStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewHTTP(reg)

	m.Request("POST /instructions", 429, time.Millisecond)
	m.Request("POST /instructions", 504, 5*time.Second)
	m.Request("POST /instructions", 504, 5*time.Second)

	if got := value(t, reg, "payrouter_http_requests_total", map[string]string{"route": "POST /instructions", "status": "429"}); got != 1 {
		t.Errorf("429 = %v, esperado 1", got)
	}
	if got := value(t, reg, "payrouter_http_requests_total", map[string]string{"route": "POST /instructions", "status": "504"}); got != 2 {
		t.Errorf("504 = %v, esperado 2", got)
	}
	if got := value(t, reg, "payrouter_http_request_duration_seconds", map[string]string{"route": "POST /instructions"}); got != 3 {
		t.Errorf("amostras de duração = %v, esperado 3", got)
	}
}

func TestParticipanteSeparaProcessadoPerdidoEDerrubadoDoQueFoiRecusado(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewParticipant(reg)

	m.Payment("accept", "accepted")
	m.Payment("drop", "dropped")
	m.Payment("crash", "crashed")
	m.Throttled()
	m.Throttled()

	if got := value(t, reg, "payrouter_participant_payments_total", map[string]string{"behavior": "drop", "result": "dropped"}); got != 1 {
		t.Errorf("dropped = %v, esperado 1", got)
	}
	if got := value(t, reg, "payrouter_participant_payments_total", map[string]string{"result": "accepted"}); got != 1 {
		t.Errorf("accepted = %v, esperado 1", got)
	}
	if got := value(t, reg, "payrouter_participant_throttled_total", nil); got != 2 {
		t.Errorf("429 = %v, esperado 2", got)
	}
}

func TestReceptorNuloDeHTTPEParticipanteNaoFazNada(t *testing.T) {
	var h *HTTP
	var p *Participant

	h.Request("GET /x", 200, time.Second)
	p.Payment("accept", "accepted")
	p.Throttled()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestServeResponde200ComAsSeriesEParaQuandoOContextoTermina(t *testing.T) {
	reg := NewRegistry()
	NewProcessor(reg).Outcome("settled")
	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, addr, reg) }()

	var body string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/metrics") //nolint:noctx // teste
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			body = string(b)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()

	if !strings.Contains(body, `payrouter_routing_outcomes_total{outcome="settled"} 1`) || !strings.Contains(body, "go_goroutines") {
		t.Errorf("corpo sem as séries esperadas:\n%s", body)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve devolveu %v ao parar, esperado nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve não parou depois do cancelamento")
	}
}

func TestServeComPortaOcupadaDevolveErroSemEsperarOContexto(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	done := make(chan error, 1)
	go func() { done <- Serve(context.Background(), l.Addr().String(), NewRegistry()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Serve = nil, esperado o erro da porta ocupada")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve ficou preso esperando o contexto, que nunca termina")
	}
}

func TestServeComEnderecoVazioDesligaAsMetricas(t *testing.T) {
	if err := Serve(context.Background(), "", NewRegistry()); err != nil {
		t.Errorf("Serve com addr vazio = %v, esperado nil e voltar na hora", err)
	}
}
