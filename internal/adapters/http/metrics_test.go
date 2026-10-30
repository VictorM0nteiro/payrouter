// metrics_test.go
// Testes das métricas HTTP da API, sobre o handler com fakes (sem banco, sem Docker).
// Objetivo: provar que o 429 rate_limited e o 504 (resultado desconhecido para o cliente) caem em
// séries diferentes, contadas pelo padrão da rota e nunca pelo caminho; que uma rota inexistente
// vira uma série só, qualquer que seja o caminho ou o método; e que as sondas não são contadas.

package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// scrape devolve o texto que o Prometheus leria em /metrics.
func scrape(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func rootWithMetrics(acc acceptor, opts []Option, timeout time.Duration) (http.Handler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	api := NewHandler(acc, &fakeGetter{}, testAuthn, testVerifier, opts...)
	return NewRoot(api, &okPinger{}, timeout, WithMetrics(metrics.NewHTTP(reg))), reg
}

func TestO429DaEntradaEO202CaemEmSeriesDiferentesPorRota(t *testing.T) {
	captureLogs(t)
	acc := &fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted}}
	h, reg := rootWithMetrics(acc, []Option{WithRateLimit(1, 1)}, time.Second)

	first := do(t, h, "POST", "/instructions", validBody, authed(map[string]string{"Idempotency-Key": "k1"}))
	second := do(t, h, "POST", "/instructions", validBody, authed(map[string]string{"Idempotency-Key": "k2"}))

	if first.Code != http.StatusAccepted || second.Code != http.StatusTooManyRequests {
		t.Fatalf("códigos %d e %d, esperado 202 e 429", first.Code, second.Code)
	}
	out := scrape(t, reg)
	for _, want := range []string{
		`payrouter_http_requests_total{route="POST /instructions",status="202"} 1`,
		`payrouter_http_requests_total{route="POST /instructions",status="429"} 1`,
		`payrouter_http_request_duration_seconds_count{route="POST /instructions"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("faltou %q em:\n%s", want, out)
		}
	}
}

func TestPrazoEstouradoApareceComo504(t *testing.T) {
	captureLogs(t)
	h, reg := rootWithMetrics(blockingAcceptor{}, nil, 50*time.Millisecond)

	rec := do(t, h, "POST", "/instructions", validBody, authed(nil))

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status %d, esperado 504", rec.Code)
	}
	if out := scrape(t, reg); !strings.Contains(out, `payrouter_http_requests_total{route="POST /instructions",status="504"} 1`) {
		t.Errorf("faltou o 504 em:\n%s", out)
	}
}

func TestRotaInexistenteViraUmaSerieSoQualquerQueSejaOCaminhoOuOMetodo(t *testing.T) {
	captureLogs(t)
	h, reg := rootWithMetrics(&fakeAcceptor{}, nil, time.Second)

	do(t, h, "GET", "/nao-existe", "", nil)
	do(t, h, "GET", "/instructions/a/b/c", "", nil)
	do(t, h, "BREW", "/qualquer-coisa", "", nil)

	out := scrape(t, reg)
	if !strings.Contains(out, `payrouter_http_requests_total{route="unmatched",status="404"} 3`) {
		t.Errorf("esperada uma série unmatched com 3, veio:\n%s", out)
	}
	for _, leaked := range []string{"nao-existe", "BREW", "a/b/c"} {
		if strings.Contains(out, leaked) {
			t.Errorf("%q vazou para um label: a cardinalidade deixou de ter teto", leaked)
		}
	}
}

func TestAsSondasNaoSaoContadas(t *testing.T) {
	captureLogs(t)
	h, reg := rootWithMetrics(&fakeAcceptor{}, nil, time.Second)

	do(t, h, "GET", "/healthz", "", nil)
	do(t, h, "GET", "/readyz", "", nil)

	if out := scrape(t, reg); strings.Contains(out, "healthz") || strings.Contains(out, "readyz") {
		t.Errorf("sonda contada nas métricas:\n%s", out)
	}
}
