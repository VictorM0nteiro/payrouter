// auth.go
// Middleware de autenticação por token Bearer.
// Objetivo: identificar QUEM está pedindo. O token vem do endpoint POST /auth/token; aqui ele
// é só verificado, e o id do cliente (claim sub) vai para o contexto. Esse id vira o scope da
// chave de idempotência (a mesma Idempotency-Key de dois clientes são duas chaves) e, mais
// adiante, a chave do rate limit. A verificação em si (assinatura, algoritmo, expiração) é
// do app.TokenVerifier; a borda não conhece JWT.

package httpapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

type clientKey struct{}

// clientFrom devolve o id do cliente autenticado, posto no contexto pelo middleware.
func clientFrom(ctx context.Context) string {
	id, _ := ctx.Value(clientKey{}).(string)
	return id
}

// bearerToken extrai o token de "Authorization: Bearer <token>". O esquema não diferencia
// maiúsculas de minúsculas (RFC 7235).
func bearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	token = strings.TrimSpace(token)
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}

// authenticate envolve um handler exigindo um token válido. É aplicado por rota, e não ao
// servidor inteiro, porque /auth/token (e depois healthz, na sessão 7) são públicos.
func authenticate(verifier app.TokenVerifier, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			writeError(w, r, errUnauthorized)
			return
		}
		clientID, err := verifier.Verify(token)
		if err != nil {
			writeError(w, r, errUnauthorized)
			return
		}
		// O log de acesso é a camada de fora e só enxerga o contexto original; este registro
		// compartilhado é como ele fica sabendo quem foi autenticado.
		if info := infoFrom(r.Context()); info != nil {
			info.clientID = clientID
		}
		next(w, r.WithContext(context.WithValue(r.Context(), clientKey{}, clientID)))
	}
}
