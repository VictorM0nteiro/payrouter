// accesslog.go
// Apoio ao log de acesso: o gravador de status e o registro mutável da requisição.
// Objetivo: emitir uma linha por requisição com método, rota, status, duração e cliente, sem
// nunca olhar corpo nem header Authorization. A rota é o padrão do ServeMux ("GET
// /instructions/{id}"), não o caminho real, para o log não ter um valor diferente por id. O
// cliente só é conhecido depois que o handler autentica, então o middleware de fora entrega
// um requestInfo no contexto e o authenticate o preenche.

package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// requestInfo reúne o que o log de acesso só descobre durante o atendimento.
type requestInfo struct {
	clientID string
}

type infoKey struct{}

func withRequestInfo(ctx context.Context, info *requestInfo) context.Context {
	return context.WithValue(ctx, infoKey{}, info)
}

// infoFrom devolve o requestInfo da requisição, ou nil fora do middleware (nos testes de
// handler que não passam por wrap).
func infoFrom(ctx context.Context) *requestInfo {
	info, _ := ctx.Value(infoKey{}).(*requestInfo)
	return info
}

// statusRecorder guarda o status escrito. Se o handler nunca chama WriteHeader, o net/http
// responde 200, que é o valor inicial.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap deixa http.ResponseController alcançar o writer de verdade (deadlines, flush).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// logAccess escreve a linha de acesso. 5xx é erro, 499 (cliente desistiu) é aviso, o resto é
// informação.
func logAccess(r *http.Request, status int, info *requestInfo, took time.Duration) {
	route := r.Pattern
	if route == "" {
		route = "unmatched"
	}
	level := slog.LevelInfo
	switch {
	case status >= 500:
		level = slog.LevelError
	case status == statusClientClosed:
		level = slog.LevelWarn
	}
	slog.Default().Log(r.Context(), level, "request",
		"method", r.Method,
		"route", route,
		"status", status,
		"duration_ms", took.Milliseconds(),
		"client_id", info.clientID,
	)
}
