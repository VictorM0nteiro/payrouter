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

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
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
}

// NewHandler devolve o http.Handler da API.
func NewHandler(accept acceptor, get getter, authn authenticator, verifier app.TokenVerifier) http.Handler {
	h := &handler{accept: accept, get: get, authn: authn}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/token", h.postToken)
	mux.HandleFunc("POST /instructions", authenticate(verifier, h.postInstruction))
	mux.HandleFunc("GET /instructions/{id}", authenticate(verifier, h.getInstruction))
	return mux
}
