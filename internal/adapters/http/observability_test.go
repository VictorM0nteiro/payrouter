// observability_test.go
// Testes do correlation ID, do log de acesso, do prazo por requisição e das sondas
// /healthz e /readyz, com casos de uso e dependências falsos, sem banco e sem Docker.
// Objetivo: provar que o id é validado antes de ir para log e header, que o log de acesso diz
// o que deve (rota, status, cliente) e nunca o que não deve (token, corpo), que o prazo
// estourado vira 504 e não 500, e que o readyz depende da dependência e o healthz não.

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/logging"
)

// captureLogs troca o logger padrão por um que escreve em buf e o restaura no fim.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(logging.New("json", &buf))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

// logLines decodifica cada linha JSON do buffer.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("linha de log inválida %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// accessLine devolve a única linha de acesso ("msg":"request") do buffer.
func accessLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, m := range logLines(t, buf) {
		if m["msg"] == "request" {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("esperada 1 linha de acesso, vieram %d: %s", len(found), buf)
	}
	return found[0]
}

func newRoot(acc acceptor, get getter, ping Pinger, timeout time.Duration) http.Handler {
	return NewRoot(NewHandler(acc, get, testAuthn, testVerifier), ping, timeout)
}

type okPinger struct{ calls int }

func (p *okPinger) Ping(context.Context) error { p.calls++; return nil }

type errPinger struct{ err error }

func (p errPinger) Ping(context.Context) error { return p.err }

// slowPinger só volta quando o contexto acaba, como um banco que não responde.
type slowPinger struct{}

func (slowPinger) Ping(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func TestCorrelationIDValido(t *testing.T) {
	tests := []struct {
		id   string
		want bool
	}{
		{"abc-123", true},
		{"A.b_c-9", true},
		{strings.Repeat("a", 64), true},
		{strings.Repeat("a", 65), false},
		{"", false},
		{"tem espaço", false},
		{"quebra\nde-linha", false},
		{"aspas\"", false},
		{"barra/x", false},
		{"acentuação", false},
	}
	for _, tc := range tests {
		if got := validCorrelationID(tc.id); got != tc.want {
			t.Errorf("validCorrelationID(%q) = %v, esperado %v", tc.id, got, tc.want)
		}
	}
}

func TestCorrelationIDReaproveitaOValidoEGeraOInvalido(t *testing.T) {
	h := newRoot(&fakeAcceptor{}, &fakeGetter{}, &okPinger{}, time.Second)

	rec := do(t, h, "GET", "/instructions/"+validUUID, "", map[string]string{"X-Correlation-ID": "meu-id-1"})
	if got := rec.Header().Get("X-Correlation-ID"); got != "meu-id-1" {
		t.Errorf("id válido deveria voltar igual, veio %q", got)
	}

	rec = do(t, h, "GET", "/instructions/"+validUUID, "", map[string]string{"X-Correlation-ID": "inválido com espaço"})
	got := rec.Header().Get("X-Correlation-ID")
	if got == "" || got == "inválido com espaço" || len(got) != 36 {
		t.Errorf("id inválido deveria virar UUID novo, veio %q", got)
	}

	rec = do(t, h, "GET", "/instructions/"+validUUID, "", nil)
	if len(rec.Header().Get("X-Correlation-ID")) != 36 {
		t.Errorf("sem header, deveria gerar UUID, veio %q", rec.Header().Get("X-Correlation-ID"))
	}
}

func TestCorrelationIDVoltaEmQualquerResposta(t *testing.T) {
	h := newRoot(&fakeAcceptor{}, &fakeGetter{}, &okPinger{}, time.Second)

	for name, req := range map[string]struct{ method, path string }{
		"401_sem_token":   {"GET", "/instructions/" + validUUID},
		"404_rota":        {"GET", "/nao-existe"},
		"405_metodo":      {"DELETE", "/instructions/" + validUUID},
		"401_credencial":  {"POST", "/auth/token"},
		"400_id_invalido": {"GET", "/instructions/abc"},
	} {
		rec := do(t, h, req.method, req.path, "", map[string]string{"X-Correlation-ID": "id-fixo"})
		if rec.Header().Get("X-Correlation-ID") != "id-fixo" {
			t.Errorf("%s: sem o header de correlação (status %d)", name, rec.Code)
		}
	}
}

func TestLogDeAcesso(t *testing.T) {
	buf := captureLogs(t)
	getter := &fakeGetter{view: app.InstructionView{ID: validUUID, Status: domain.StatusAccepted}}
	h := newRoot(&fakeAcceptor{}, getter, &okPinger{}, time.Second)

	do(t, h, "GET", "/instructions/"+validUUID, "", map[string]string{
		"Authorization":    "Bearer tok-A",
		"X-Correlation-ID": "corr-1",
	})

	line := accessLine(t, buf)
	want := map[string]any{
		"method":         "GET",
		"route":          "GET /instructions/{id}",
		"status":         float64(200),
		"client_id":      "clienteA",
		"correlation_id": "corr-1",
		"level":          "INFO",
	}
	for k, v := range want {
		if line[k] != v {
			t.Errorf("%s = %v, esperado %v (linha %v)", k, line[k], v, line)
		}
	}
	if _, ok := line["duration_ms"]; !ok {
		t.Error("falta duration_ms")
	}
	if strings.Contains(buf.String(), validUUID) {
		t.Error("a rota deve ser o padrão, o caminho real com o id não pode estar no log")
	}
}

func TestLogDeAcessoNaoVazaTokenNemCorpo(t *testing.T) {
	buf := captureLogs(t)
	h := newRoot(&fakeAcceptor{result: app.AcceptInstructionResult{ID: validUUID, Status: domain.StatusAccepted}}, &fakeGetter{}, &okPinger{}, time.Second)

	do(t, h, "POST", "/instructions", `{"debtor":"SEGREDO-DO-CORPO","creditor":"c","amount_cents":15000,"end_to_end_id":"e"}`,
		map[string]string{"Authorization": "Bearer tok-A", "Idempotency-Key": "k1"})

	out := buf.String()
	for _, secret := range []string{"tok-A", "SEGREDO-DO-CORPO", "Bearer", "15000"} {
		if strings.Contains(out, secret) {
			t.Errorf("o log contém %q:\n%s", secret, out)
		}
	}
}

func TestLogDeAcessoSemAutenticacaoTemClienteVazioEUnmatchedParaRotaInexistente(t *testing.T) {
	buf := captureLogs(t)
	h := newRoot(&fakeAcceptor{}, &fakeGetter{}, &okPinger{}, time.Second)

	do(t, h, "GET", "/instructions/"+validUUID, "", nil)
	line := accessLine(t, buf)
	if line["status"] != float64(401) || line["client_id"] != "" || line["level"] != "INFO" {
		t.Errorf("401: %v", line)
	}

	buf.Reset()
	do(t, h, "GET", "/nao-existe", "", nil)
	if line := accessLine(t, buf); line["status"] != float64(404) {
		t.Errorf("rota inexistente: %v", line)
	}
}

func TestErroInternoVaiParaOLogComCorrelationIDENaoParaOCliente(t *testing.T) {
	buf := captureLogs(t)
	h := newRoot(&fakeAcceptor{err: errors.New("pq: senha do banco inválida")}, &fakeGetter{}, &okPinger{}, time.Second)

	rec := do(t, h, "POST", "/instructions", validBody,
		map[string]string{"Authorization": "Bearer tok-A", "Idempotency-Key": "k1", "X-Correlation-ID": "corr-9"})

	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "senha") {
		t.Fatalf("status %d, corpo %s", rec.Code, rec.Body)
	}
	var internal, access map[string]any
	for _, m := range logLines(t, buf) {
		switch m["msg"] {
		case "erro interno":
			internal = m
		case "request":
			access = m
		}
	}
	if internal == nil || internal["correlation_id"] != "corr-9" || !strings.Contains(internal["error"].(string), "senha") {
		t.Errorf("registro do erro interno = %v", internal)
	}
	if access["level"] != "ERROR" || access["status"] != float64(500) {
		t.Errorf("a linha de acesso de um 5xx deve ser ERROR: %v", access)
	}
}

// blockingAcceptor só volta quando o contexto acaba, como um banco que não responde.
type blockingAcceptor struct{}

func (blockingAcceptor) Execute(ctx context.Context, _ app.AcceptInstructionInput) (app.AcceptInstructionResult, error) {
	<-ctx.Done()
	return app.AcceptInstructionResult{}, ctx.Err()
}

func TestPrazoEstouradoVira504(t *testing.T) {
	captureLogs(t)
	h := newRoot(blockingAcceptor{}, &fakeGetter{}, &okPinger{}, 50*time.Millisecond)

	start := time.Now()
	rec := do(t, h, "POST", "/instructions", validBody, authed(nil))

	if rec.Code != http.StatusGatewayTimeout || errorCode(t, rec) != "request_timeout" {
		t.Fatalf("status %d código %q", rec.Code, errorCode(t, rec))
	}
	if !strings.Contains(rec.Body.String(), "Idempotency-Key") {
		t.Errorf("a mensagem deve orientar a repetir com a mesma chave: %s", rec.Body)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("o prazo de 50ms demorou %v", took)
	}
}

func TestClienteQueDesisteVira499EAvisoNoLog(t *testing.T) {
	buf := captureLogs(t)
	h := newRoot(blockingAcceptor{}, &fakeGetter{}, &okPinger{}, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "/instructions", strings.NewReader(validBody)).WithContext(ctx)
	for k, v := range authed(nil) {
		req.Header.Set(k, v)
	}
	cancel()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 499 || errorCode(t, rec) != "request_canceled" {
		t.Fatalf("status %d código %q", rec.Code, errorCode(t, rec))
	}
	if line := accessLine(t, buf); line["level"] != "WARN" {
		t.Errorf("cliente que desiste é WARN, não ERROR: %v", line)
	}
}

func TestHealthzNaoDependeDeNada(t *testing.T) {
	buf := captureLogs(t)
	ping := &okPinger{}
	h := newRoot(&fakeAcceptor{}, &fakeGetter{}, ping, time.Second)

	rec := do(t, h, "GET", "/healthz", "", nil)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Errorf("status %d corpo %s", rec.Code, rec.Body)
	}
	if ping.calls != 0 {
		t.Error("healthz não pode tocar na dependência")
	}
	if strings.TrimSpace(buf.String()) != "" {
		t.Errorf("sonda não gera log de acesso: %s", buf)
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name       string
		ping       Pinger
		wantStatus int
	}{
		{"dependencia_ok", &okPinger{}, 200},
		{"dependencia_com_erro", errPinger{errors.New("connection refused 10.0.0.5:5432")}, 503},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			captureLogs(t)
			h := newRoot(&fakeAcceptor{}, &fakeGetter{}, tc.ping, time.Second)

			rec := do(t, h, "GET", "/readyz", "", nil)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, esperado %d", rec.Code, tc.wantStatus)
			}
			if strings.Contains(rec.Body.String(), "10.0.0.5") {
				t.Errorf("o 503 vazou o motivo: %s", rec.Body)
			}
		})
	}
}

func TestReadyzComPingLentoEIndisponivel(t *testing.T) {
	captureLogs(t)
	h := NewOps(slowPinger{}, 50*time.Millisecond)

	start := time.Now()
	rec := do(t, h, "GET", "/readyz", "", nil)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, esperado 503", rec.Code)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("o prazo do ping de 50ms demorou %v", took)
	}
}

func TestSondasSoAceitamGet(t *testing.T) {
	h := newRoot(&fakeAcceptor{}, &fakeGetter{}, &okPinger{}, time.Second)

	if rec := do(t, h, "POST", "/healthz", "", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d", rec.Code)
	}
}
