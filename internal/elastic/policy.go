// policy.go
// A política que decide quando o pool cresce ou encolhe, como função pura de uma amostra.
// Objetivo: separar a decisão (testável sem goroutines nem relógio) da mecânica de aplicá-la. A
// histerese vem de duas coisas: limiares diferentes para crescer e para encolher, e um número
// de amostras seguidas muito maior para encolher do que para crescer, mais uma pausa depois de
// cada mudança. Crescer rápido e encolher devagar evita oscilar numa carga que vai e volta.
package elastic

import "time"

// Policy ajusta a decisão de redimensionar. A Policy zerada usa DefaultPolicy; numa Policy
// parcial, os campos zerados usam o padrão, exceto Cooldown, onde 0 é "sem pausa".
type Policy struct {
	GrowWait    time.Duration // espera acima da qual há pressão (tarefa pronta que demora a começar)
	GrowTicks   int           // amostras seguidas com pressão antes de crescer um worker
	ShrinkBelow float64       // fração de workers ocupados abaixo da qual há ociosidade
	ShrinkTicks int           // amostras seguidas ociosas antes de encolher um worker
	Cooldown    int           // amostras ignoradas depois de qualquer mudança de tamanho
}

// DefaultPolicy é conservadora: cresce depressa, encolhe devagar.
func DefaultPolicy() Policy {
	return Policy{GrowWait: 20 * time.Millisecond, GrowTicks: 2, ShrinkBelow: 0.3, ShrinkTicks: 10, Cooldown: 2}
}

func (p Policy) withDefaults() Policy {
	d := DefaultPolicy()
	if p == (Policy{}) {
		return d
	}
	if p.GrowWait <= 0 {
		p.GrowWait = d.GrowWait
	}
	if p.GrowTicks <= 0 {
		p.GrowTicks = d.GrowTicks
	}
	if p.ShrinkBelow <= 0 {
		p.ShrinkBelow = d.ShrinkBelow
	}
	if p.ShrinkTicks <= 0 {
		p.ShrinkTicks = d.ShrinkTicks
	}
	return p
}

// Sample é o que o controlador observa a cada intervalo.
type Sample struct {
	Size  int           // tamanho desejado agora
	Min   int           // piso
	Max   int           // teto
	Ready int           // tarefas prontas para rodar (chave livre) que ainda não começaram
	Wait  time.Duration // espera: a maior entre a média das que começaram e a idade da mais antiga pronta
	Busy  float64       // fração de workers ocupados, de 0 a 1
}

// sizer guarda a contagem de amostras seguidas. Não é seguro para uso concorrente: só o
// controlador o usa.
type sizer struct {
	p      Policy
	grow   int
	shrink int
	cool   int
}

func newSizer(p Policy) *sizer { return &sizer{p: p.withDefaults()} }

// next devolve +1 (crescer um worker), -1 (encolher um) ou 0.
func (s *sizer) next(x Sample) int {
	if s.cool > 0 {
		s.cool--
		return 0
	}
	loaded := x.Ready > 0 && x.Wait > s.p.GrowWait
	idle := x.Ready == 0 && x.Busy < s.p.ShrinkBelow
	switch {
	case loaded:
		s.grow++
		s.shrink = 0
	case idle:
		s.shrink++
		s.grow = 0
	default:
		s.grow, s.shrink = 0, 0
	}
	if s.grow >= s.p.GrowTicks && x.Size < x.Max {
		s.grow, s.cool = 0, s.p.Cooldown
		return 1
	}
	if s.shrink >= s.p.ShrinkTicks && x.Size > x.Min {
		s.shrink, s.cool = 0, s.p.Cooldown
		return -1
	}
	return 0
}
