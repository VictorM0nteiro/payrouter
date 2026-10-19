// logging_test.go
// Testes do logger e do correlation ID no contexto, sem dependências externas.
// Objetivo: provar que o id só aparece quando está no contexto, que sobrevive a loggers
// derivados (With/WithGroup) e que o formato escolhido é o que sai.

package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("saída não é JSON: %q (%v)", buf.String(), err)
	}
	return m
}

func TestCorrelationIDNoContexto(t *testing.T) {
	if got := CorrelationID(context.Background()); got != "" {
		t.Errorf("contexto vazio devolveu %q", got)
	}
	ctx := WithCorrelationID(context.Background(), "abc-123")
	if got := CorrelationID(ctx); got != "abc-123" {
		t.Errorf("CorrelationID = %q", got)
	}
}

func TestLogCarregaOCorrelationIDDoContexto(t *testing.T) {
	var buf bytes.Buffer
	log := New("json", &buf)

	log.InfoContext(WithCorrelationID(context.Background(), "abc-123"), "olá", "k", "v")

	m := decode(t, &buf)
	if m["correlation_id"] != "abc-123" || m["msg"] != "olá" || m["k"] != "v" {
		t.Errorf("registro = %v", m)
	}
}

func TestLogSemCorrelationIDNaoInventaOCampo(t *testing.T) {
	var buf bytes.Buffer
	New("json", &buf).Info("olá")

	if _, ok := decode(t, &buf)["correlation_id"]; ok {
		t.Error("sem id no contexto, o campo não deveria existir")
	}
}

func TestLoggerDerivadoMantemOCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	log := New("json", &buf).With("componente", "api").WithGroup("g")

	log.InfoContext(WithCorrelationID(context.Background(), "abc-123"), "olá")

	m := decode(t, &buf)
	// Depois de WithGroup, os atributos novos entram dentro do grupo; o ponto é que o
	// registro continua carregando o id (em qualquer nível) e o atributo do With.
	if !strings.Contains(buf.String(), "abc-123") || m["componente"] != "api" {
		t.Errorf("registro = %v", m)
	}
}

func TestFormatoText(t *testing.T) {
	var buf bytes.Buffer
	New("text", &buf).InfoContext(WithCorrelationID(context.Background(), "abc-123"), "olá")

	out := buf.String()
	if strings.HasPrefix(out, "{") || !strings.Contains(out, "msg=olá") || !strings.Contains(out, "correlation_id=abc-123") {
		t.Errorf("saída = %q", out)
	}
}

func TestFormatoDesconhecidoCaiEmJSON(t *testing.T) {
	var buf bytes.Buffer
	New("xml", &buf).Info("olá")

	decode(t, &buf)
}
