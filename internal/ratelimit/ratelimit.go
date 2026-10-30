// ratelimit.go
// Balde de fichas (token bucket) portado do wallet-go, que usa golang.org/x/time/rate com
// Reserve + Delay + Cancel (wallet-go/internal/adapters/http/ratelimit.go). É reuso declarado,
// não contribuição deste projeto.
// Objetivo: dizer a quem pede quanto precisa esperar, sem bloquear. O mesmo balde serve ao limite
// de ENTRADA (por cliente, na API) e ao limite de SAÍDA (rumo ao participante, no processor).

package ratelimit

import (
	"time"

	"golang.org/x/time/rate"
)

// Bucket permite rps eventos por segundo, com rajadas de até burst. É seguro para uso concorrente.
type Bucket struct{ l *rate.Limiter }

// New cria o balde. rps e burst devem ser positivos; quem quer o limite desligado não cria balde.
func New(rps float64, burst int) *Bucket {
	return &Bucket{l: rate.NewLimiter(rate.Limit(rps), burst)}
}

// Reserve devolve 0 se o evento pode acontecer agora (e consome uma ficha) ou, se não puder,
// quanto esperar até haver ficha. Quando manda esperar, a reserva é devolvida: a ficha só é
// consumida quando o chamador volta e recebe 0. Isso evita cobrar duas fichas por um único evento
// que foi adiado e repetido, como faz o consumidor ao repetir a mesma mensagem.
//
// shortcut: vários chamadores concorrentes mandados esperar acordam juntos e disputam a mesma
// ficha. Para um consumidor sequencial não importa; revisitar quando o pool elástico (sessões
// 18 e 19) processar em paralelo.
func (b *Bucket) Reserve() time.Duration {
	res := b.l.Reserve()
	if !res.OK() {
		// burst maior do que o balde comporta: erro de configuração, não do chamador. Deixa passar
		// em vez de recusar tudo para sempre (mesma escolha do wallet-go).
		return 0
	}
	d := res.Delay()
	if d > 0 {
		res.Cancel()
	}
	return d
}
