// ops.go
// Endpoints operacionais: /healthz (liveness) e /readyz (readiness).
// Objetivo: o orquestrador separa duas perguntas. "O processo está vivo?" (healthz) nunca
// depende de nada externo: se o Postgres cair, reiniciar a API não resolve, então ela não pode
// ser derrubada por isso. "Posso receber tráfego?" (readyz) depende do Postgres: se falha, a
// API sai do balanceamento sem ser reiniciada. São públicos e não geram linha de log de
// acesso, porque a sonda bate a cada poucos segundos. O corpo do 503 não diz o motivo; ele
// vai para o log.

package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// Pinger é a dependência que o readyz confere. *pgxpool.Pool a satisfaz.
type Pinger interface {
	Ping(ctx context.Context) error
}

type statusBody struct {
	Status string `json:"status"`
}

// NewOps devolve o handler de /healthz e /readyz. readyTimeout limita o ping.
func NewOps(ready Pinger, readyTimeout time.Duration) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := ready.Ping(ctx); err != nil {
			slog.WarnContext(r.Context(), "readyz: dependência indisponível", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, statusBody{Status: "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
	})

	return mux
}
