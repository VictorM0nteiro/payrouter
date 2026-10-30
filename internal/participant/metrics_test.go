// metrics_test.go
// Testes das métricas do participante simulado, sobre HTTP de verdade (httptest), sem Docker.
// Objetivo: provar que o participante conta o que de fato aconteceu do lado dele, que é a verdade
// com que o processor é comparado: aceito, rejeitado, perdido (drop), caiu (crash) e 429. Uma
// instrução repetida é contada uma vez só, como é processada uma vez só.

package participant

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func scrape(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func withMetrics(cfg Config) (Config, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	cfg.Metrics = metrics.NewParticipant(reg)
	return cfg, reg
}

// mustPay envia uma instrução e falha o teste se a chamada der erro.
func mustPay(t *testing.T, srv *httptest.Server, id, e2e string) {
	t.Helper()
	if _, _, err := pay(t, srv, 2*time.Second, body(id, e2e)); err != nil {
		t.Fatal(err)
	}
}

func wantSeries(t *testing.T, out string, lines ...string) {
	t.Helper()
	for _, want := range lines {
		if !strings.Contains(out, want) {
			t.Errorf("faltou %q em:\n%s", want, out)
		}
	}
}

func TestContaAceitasERejeitadasPorComportamento(t *testing.T) {
	cfg, reg := withMetrics(fastConfig())
	srv := newServer(t, cfg)

	mustPay(t, srv, "m1", "accept:1")
	mustPay(t, srv, "m2", "accept:2")
	mustPay(t, srv, "m3", "reject:1")

	wantSeries(t, scrape(t, reg),
		`payrouter_participant_payments_total{behavior="accept",result="accepted"} 2`,
		`payrouter_participant_payments_total{behavior="reject",result="rejected"} 1`,
	)
}

func TestInstrucaoRepetidaEContadaUmaVezSo(t *testing.T) {
	cfg, reg := withMetrics(fastConfig())
	srv := newServer(t, cfg)

	mustPay(t, srv, "m4", "reject:1")
	mustPay(t, srv, "m4", "accept:1") // reenvio: devolve o guardado

	out := scrape(t, reg)
	wantSeries(t, out, `payrouter_participant_payments_total{behavior="reject",result="rejected"} 1`)
	if strings.Contains(out, `behavior="accept"`) {
		t.Errorf("a repetição foi contada como processada de novo:\n%s", out)
	}
}

func TestDropECrashSaoContadosEnaoViramProcessados(t *testing.T) {
	var exited atomic.Bool
	cfg, reg := withMetrics(fastConfig())
	cfg.Exit = func(int) { exited.Store(true) }
	srv := newServer(t, cfg)

	_, _, _ = pay(t, srv, 200*time.Millisecond, body("m5", "drop:1"))                                       // o cliente desiste
	resp, err := http.Post(srv.URL+"/payments", "application/json", bytes.NewReader(body("m6", "crash:1"))) //nolint:noctx // teste
	if err == nil {
		_ = resp.Body.Close()
	}

	out := scrape(t, reg)
	wantSeries(t, out,
		`payrouter_participant_payments_total{behavior="drop",result="dropped"} 1`,
		`payrouter_participant_payments_total{behavior="crash",result="crashed"} 1`,
	)
	if strings.Contains(out, `result="accepted"`) || strings.Contains(out, `result="rejected"`) {
		t.Errorf("drop e crash não processam nada:\n%s", out)
	}
	if !exited.Load() {
		t.Error("o Exit injetado não foi chamado")
	}
}

func TestOs429ContadosBatemComOContadorThrottledDoStatus(t *testing.T) {
	cfg, reg := withMetrics(fastConfig())
	cfg.TPS, cfg.Burst = 2, 2
	srv := newServer(t, cfg)

	seen := 0
	for i := range 5 {
		id := fmt.Sprintf("m7-%d", i)
		mustPay(t, srv, id, "accept:1")
		_, st := status(t, srv, id)
		seen += int(st["throttled"].(float64))
	}

	if seen == 0 {
		t.Fatal("nenhum 429 aconteceu, o teste não provaria nada")
	}
	wantSeries(t, scrape(t, reg), fmt.Sprintf("payrouter_participant_throttled_total %d", seen))
}
