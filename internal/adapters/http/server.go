// server.go
// Montagem das rotas HTTP e as interfaces que a borda exige da aplicação.
// Objetivo: ser o ponto de entrada do adapter. Os handlers dependem de interfaces pequenas
// definidas aqui (acceptor, getter, authenticator), não dos tipos concretos de internal/app,
// então podem ser testados com fakes, sem banco. As rotas de instrução exigem token; a de
// autenticação é pública.

package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/logging"
)

// acceptor é o que o handler de POST /instructions precisa do caso de uso.
type acceptor interface {
	Execute(ctx context.Context, in app.AcceptInstructionInput) (app.AcceptInstructionResult, error)
}

// getter é o que o handler de GET /instructions/{id} precisa do caso de uso.
type getter interface {
	Execute(ctx context.Context, id domain.InstructionID) (app.InstructionView, error)
}

// authenticator é o que o handler de POST /auth/token precisa do caso de uso.
type authenticator interface {
	Execute(ctx context.Context, clientID, secret string) (app.AuthToken, error)
}

type handler struct {
	accept acceptor
	get    getter
	authn  authenticator
	limits *clientLimiter // limite de entrada por cliente; nil = sem limite
}

// readyTimeout é o prazo do ping do /readyz: uma sonda lenta conta como indisponível.
const readyTimeout = 2 * time.Second

// NewRoot junta tudo no handler que o http.Server serve: as sondas (sem log de acesso, sem
// autenticação) e a API embrulhada em correlation ID, prazo por requisição e log de acesso.
func NewRoot(api http.Handler, ready Pinger, requestTimeout time.Duration) http.Handler {
	ops := NewOps(ready, readyTimeout)

	root := http.NewServeMux()
	// Sem método no padrão: o mux de ops é quem responde 405 a um POST nas sondas.
	root.Handle("/healthz", ops)
	root.Handle("/readyz", ops)
	root.Handle("/", wrap(api, requestTimeout))
	return root
}

// wrap aplica, numa única passada, o que toda requisição da API precisa: correlation ID (no
// contexto, no log e no header da resposta), o prazo e o log de acesso. É uma função só, e
// não três middlewares empilhados, porque o log de acesso precisa ler r.Pattern depois do
// atendimento e o ServeMux o grava na requisição que ele recebe: tem de ser a mesma.
func wrap(next http.Handler, requestTimeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := correlationIDFor(r)
		w.Header().Set(correlationHeader, id)

		ctx, cancel := context.WithTimeout(logging.WithCorrelationID(r.Context(), id), requestTimeout)
		defer cancel()
		info := &requestInfo{}
		r = r.WithContext(withRequestInfo(ctx, info))

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		logAccess(r, rec.status, info, time.Since(start))
	})
}

// NewHandler devolve o http.Handler da API.
func NewHandler(accept acceptor, get getter, authn authenticator, verifier app.TokenVerifier, opts ...Option) http.Handler {
	h := &handler{accept: accept, get: get, authn: authn}
	for _, opt := range opts {
		opt(h)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/token", h.postToken)
	mux.HandleFunc("POST /instructions", authenticate(verifier, limit(h.limits, h.postInstruction)))
	mux.HandleFunc("GET /instructions/{id}", authenticate(verifier, limit(h.limits, h.getInstruction)))
	return mux
}
