// ratelimit.go
// Limite de taxa de ENTRADA por cliente autenticado, em cima do balde de fichas portado do
// wallet-go (internal/ratelimit). É reuso declarado, não contribuição deste projeto.
// Objetivo: impedir que um cliente sozinho encha a API. Passou do limite, responde 429
// "rate_limited" com Retry-After, sem executar o caso de uso. Atenção: este 429 (API -> cliente)
// não tem relação com o 429 do participante (processor -> participante), que significa "não
// processado, pode reenviar" e vira RoutingThrottled.

package httpapi

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/ratelimit"
)

var errRateLimited = errors.New("limite de requisições excedido")

// Option ajusta o handler da API.
type Option func(*handler)

// WithRateLimit liga o limite de entrada: rps requisições por segundo por cliente, com rajada de
// até burst. rps <= 0 deixa desligado, que é o padrão.
func WithRateLimit(rps float64, burst int) Option {
	return func(h *handler) {
		if rps > 0 {
			h.limits = newClientLimiter(rps, max(1, burst))
		}
	}
}

// clientLimiter guarda um balde por cliente, criado na primeira requisição dele.
//
// shortcut: os baldes nunca são removidos. Os clientes são cadastrados por um administrador
// (clientctl), então o conjunto é pequeno e limitado; se um dia o cadastro for aberto, trocar por
// um mapa com expiração dos ociosos.
type clientLimiter struct {
	rps     float64
	burst   int
	mu      sync.Mutex
	buckets map[string]*ratelimit.Bucket
}

func newClientLimiter(rps float64, burst int) *clientLimiter {
	return &clientLimiter{rps: rps, burst: burst, buckets: make(map[string]*ratelimit.Bucket)}
}

// reserve devolve 0 se o cliente pode passar ou quanto deve esperar.
func (c *clientLimiter) reserve(client string) time.Duration {
	c.mu.Lock()
	b, ok := c.buckets[client]
	if !ok {
		b = ratelimit.New(c.rps, c.burst)
		c.buckets[client] = b
	}
	c.mu.Unlock()
	return b.Reserve()
}

// limit recusa com 429 o cliente que passou do limite. Roda DEPOIS de authenticate, que põe o
// cliente no contexto; sem limites configurados, devolve o próprio handler.
func limit(c *clientLimiter, next http.HandlerFunc) http.HandlerFunc {
	if c == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if wait := c.reserve(clientFrom(r.Context())); wait > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(wait.Seconds())))))
			writeError(w, r, errRateLimited)
			return
		}
		next(w, r)
	}
}
