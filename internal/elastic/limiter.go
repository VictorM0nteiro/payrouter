// limiter.go
// O limitador adaptativo de requisições em voo rumo ao participante: um semáforo cujo tamanho
// sobe e desce pela regra AIMD (aimd.go).
// Objetivo: o participante não informa a sua capacidade; ela se descobre pelas respostas. O limite
// é o teto de requisições simultâneas, não de requisições por segundo (esse é o ratelimit.Bucket, e
// os dois se somam). O limitador nunca reenvia nada: um Timeout só reduz o limite.
//
// Uma queda por rodada: uma vaga guarda a "geração" em que foi tomada, e só a primeira falha de
// cada geração reduz o limite. Dez timeouts de requisições que já estavam em voo juntas contam
// como um evento e dividem o limite uma vez, não dez.

package elastic

import (
	"context"
	"sync"
)

// LimiterConfig parametriza o limitador. Min e Max valem pelo menos 1 e Max >= Min; Initial fica
// entre os dois (padrão Min); Beta fica em (0, 1) (padrão 0,5).
type LimiterConfig struct {
	Min     int
	Max     int
	Initial int
	Beta    float64

	// OnChange é chamada, fora do mutex, quando o limite inteiro muda. Duas chamadas de
	// goroutines diferentes podem chegar fora de ordem. Opcional.
	OnChange func(from, to int)
}

// LimiterStats é um retrato do limitador.
type LimiterStats struct {
	Limit    int // requisições simultâneas permitidas agora
	InFlight int // vagas tomadas
	Waiting  int // chamadores esperando uma vaga
}

type waiter struct {
	ready   chan struct{}
	gen     uint64
	granted bool
}

// Limiter é um semáforo de tamanho adaptativo. Crie com NewLimiter.
type Limiter struct {
	rule     aimd
	onChange func(from, to int)

	mu       sync.Mutex
	limit    float64
	inflight int
	gen      uint64
	queue    []*waiter //FIFO
}

// NewLimiter cria o limitador já no tamanho inicial.
func NewLimiter(cfg LimiterConfig) *Limiter {
	if cfg.Min < 1 {
		cfg.Min = 1
	}
	if cfg.Max < cfg.Min {
		cfg.Max = cfg.Min
	}
	if cfg.Initial < cfg.Min || cfg.Initial > cfg.Max {
		cfg.Initial = cfg.Min
	}
	if cfg.Beta <= 0 || cfg.Beta >= 1 {
		cfg.Beta = 0.5
	}
	return &Limiter{
		rule:     aimd{min: float64(cfg.Min), max: float64(cfg.Max), beta: cfg.Beta},
		onChange: cfg.OnChange,
		limit:    float64(cfg.Initial),
	}
}

// Acquire espera uma vaga e devolve a função que a devolve com o resultado da requisição. Chamar
// a função de novo não faz nada. Se ctx terminar antes, devolve o erro dele sem tomar vaga.
func (l *Limiter) Acquire(ctx context.Context) (func(Outcome), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if len(l.queue) == 0 && l.inflight < l.capLocked() {
		l.inflight++
		gen := l.gen
		l.mu.Unlock()
		return l.releaser(gen), nil
	}
	w := &waiter{ready: make(chan struct{})}
	l.queue = append(l.queue, w)
	l.mu.Unlock()

	select {
	case <-w.ready:
		return l.releaser(w.gen), nil
	case <-ctx.Done():
		l.mu.Lock()
		if w.granted {
			// A vaga já era nossa: devolve sem contar como resultado.
			gen := w.gen
			l.mu.Unlock()
			l.done(gen, Success, false)
			return nil, ctx.Err()
		}
		for i, q := range l.queue {
			if q == w {
				l.queue = append(l.queue[:i], l.queue[i+1:]...)
				break
			}
		}
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Stats devolve um retrato do limitador.
func (l *Limiter) Stats() LimiterStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return LimiterStats{Limit: l.capLocked(), InFlight: l.inflight, Waiting: len(l.queue)}
}

func (l *Limiter) releaser(gen uint64) func(Outcome) {
	var once sync.Once
	return func(o Outcome) { once.Do(func() { l.done(gen, o, o != Unused) }) }
}

// done devolve uma vaga. Se count, o resultado também move o limite.
func (l *Limiter) done(gen uint64, o Outcome, count bool) {
	l.mu.Lock()
	l.inflight--
	from := l.capLocked()
	if count {
		switch {
		case o == Success:
			l.limit = l.rule.next(l.limit, o)
		case gen == l.gen: // a primeira falha da geração reduz; as outras já estavam em voo
			l.gen++
			l.limit = l.rule.next(l.limit, o)
		}
	}
	to := l.capLocked()
	l.dispatchLocked()
	l.mu.Unlock()
	if from != to && l.onChange != nil {
		l.onChange(from, to)
	}
}

// dispatchLocked entrega vagas aos que esperam, em ordem, enquanto houver capacidade.
func (l *Limiter) dispatchLocked() {
	for len(l.queue) > 0 && l.inflight < l.capLocked() {
		w := l.queue[0]
		l.queue[0] = nil
		l.queue = l.queue[1:]
		l.inflight++
		w.gen, w.granted = l.gen, true
		close(w.ready)
	}
}

func (l *Limiter) capLocked() int { return max(1, int(l.limit)) }
