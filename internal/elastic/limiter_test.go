// limiter_test.go
// Testes do limitador adaptativo de requisições em voo, sob -race.
// Objetivo: provar que nunca há mais requisições em voo do que o limite, que o limite cai uma vez
// por rodada (dez timeouts juntos dividem uma vez), que reduzir o limite com vagas seguradas não
// derruba ninguém e só libera quem espera quando o excesso drena, que a espera sai em ordem e
// termina com o contexto sem vazar vaga, e que devolver duas vezes não conta duas.

package elastic

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func acquire(t *testing.T, l *Limiter) func(Outcome) {
	t.Helper()
	release, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return release
}

// waitWaiting espera até n chamadores estarem na fila.
func waitWaiting(t *testing.T, l *Limiter, n int) {
	t.Helper()
	waitFor(t, 2*time.Second, "chamadores na fila", func() bool { return l.Stats().Waiting == n })
}

func TestNuncaHaMaisEmVooDoQueOLimite(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 3, Max: 3})
	var cur, peak atomic.Int64
	var wg sync.WaitGroup

	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release := acquire(t, l)
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			cur.Add(-1)
			release(Success)
		}()
	}
	wg.Wait()

	if peak.Load() > 3 {
		t.Errorf("pico de %d requisições em voo com limite 3", peak.Load())
	}
	if peak.Load() < 2 {
		t.Errorf("pico de %d: o limitador não deixou paralelizar", peak.Load())
	}
	if s := l.Stats(); s.InFlight != 0 || s.Waiting != 0 {
		t.Errorf("sobrou estado: %+v", s)
	}
}

func TestDezTimeoutsDeUmaMesmaRodadaDividemOLimiteUmaVez(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 1, Max: 8, Initial: 8})
	releases := make([]func(Outcome), 8)
	for i := range releases {
		releases[i] = acquire(t, l)
	}

	for _, r := range releases {
		r(Timeout)
	}

	if got := l.Stats().Limit; got != 4 {
		t.Errorf("limite %d depois de 8 timeouts juntos, esperado 4 (uma divisão)", got)
	}
}

func TestReduzirOLimiteComVagasSeguradasSoLiberaQuemEsperaQuandoOExcessoDrena(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 1, Max: 8, Initial: 8})
	releases := make([]func(Outcome), 8)
	for i := range releases {
		releases[i] = acquire(t, l)
	}
	granted := make(chan struct{})
	go func() {
		r := acquire(t, l)
		close(granted)
		r(Success)
	}()
	waitWaiting(t, l, 1)

	releases[0](Timeout) // limite 8 -> 4, com 7 em voo
	for _, r := range releases[1:5] {
		r(Success) // 7 -> 3 em voo... mas só depois do 4º (7,6,5,4) ainda não há vaga
		select {
		case <-granted:
			if l.Stats().InFlight > 4 {
				t.Fatalf("o que esperava entrou com %d em voo e limite 4", l.Stats().InFlight)
			}
		default:
		}
	}
	for _, r := range releases[5:] {
		r(Success)
	}

	select {
	case <-granted:
	case <-time.After(2 * time.Second):
		t.Fatal("quem esperava nunca recebeu a vaga depois de o excesso drenar")
	}
}

func TestAcquireVoltaComOContextoSemTomarVaga(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 1, Max: 1})
	held := acquire(t, l)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	release, err := l.Acquire(ctx)

	if err == nil || release != nil {
		t.Fatalf("Acquire devolveu release=%v, err=%v; esperado nil e o erro do contexto", release != nil, err)
	}
	if s := l.Stats(); s.Waiting != 0 || s.InFlight != 1 {
		t.Errorf("estado %+v; esperado 0 esperando e só a vaga segurada", s)
	}
	held(Success)
	acquire(t, l)(Success) // a vaga não vazou: entra na hora
}

func TestEsperaECancelamentoNaoVazamVaga(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 2, Max: 2})
	var wg sync.WaitGroup

	for i := range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.Intn(3000))*time.Microsecond)
			defer cancel()
			release, err := l.Acquire(ctx)
			if err != nil {
				return
			}
			time.Sleep(time.Duration(i%3) * 300 * time.Microsecond)
			release(Success)
		}()
	}
	wg.Wait()

	if s := l.Stats(); s.InFlight != 0 || s.Waiting != 0 {
		t.Errorf("vazou vaga ou espera: %+v", s)
	}
}

func TestQuemEsperaEntraEmOrdem(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 1, Max: 1})
	held := acquire(t, l)
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup

	for i := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := acquire(t, l)
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			r(Success)
		}()
		waitWaiting(t, l, i+1) // um por vez na fila, para a ordem de chegada ser conhecida
	}
	held(Success)
	wg.Wait()

	for i, got := range order {
		if got != i {
			t.Fatalf("ordem de entrada %v, esperado 0..4", order)
		}
	}
}

func TestDevolverDuasVezesContaUmaSo(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 1, Max: 8, Initial: 8})
	a, b := acquire(t, l), acquire(t, l)

	a(Success)
	a(Timeout) // repetida: ignorada

	if s := l.Stats(); s.InFlight != 1 || s.Limit != 8 {
		t.Errorf("estado %+v; esperado 1 em voo e limite 8", s)
	}
	b(Success)
}

func TestVagaNaoUsadaNaoMoveOLimite(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 1, Max: 8, Initial: 4})

	for range 100 {
		acquire(t, l)(Unused)
	}

	if s := l.Stats(); s.Limit != 4 || s.InFlight != 0 {
		t.Errorf("estado %+v; devolver sem usar não pode subir nem descer o limite", s)
	}
}

func TestLimiteVoltaASubirDepoisDeCair(t *testing.T) {
	l := NewLimiter(LimiterConfig{Min: 1, Max: 8, Initial: 8})
	acquire(t, l)(Throttled)
	if got := l.Stats().Limit; got != 4 {
		t.Fatalf("limite %d depois do 429, esperado 4", got)
	}

	for range 200 {
		acquire(t, l)(Success)
	}

	if got := l.Stats().Limit; got != 8 {
		t.Errorf("limite %d depois de muitos sucessos, esperado voltar a 8", got)
	}
}

func TestOnChangeRecebeODeEOPara(t *testing.T) {
	var mu sync.Mutex
	var changes [][2]int
	l := NewLimiter(LimiterConfig{Min: 1, Max: 8, Initial: 8, OnChange: func(from, to int) {
		mu.Lock()
		changes = append(changes, [2]int{from, to})
		mu.Unlock()
	}})

	acquire(t, l)(Timeout)

	mu.Lock()
	defer mu.Unlock()
	if len(changes) != 1 || changes[0] != [2]int{8, 4} {
		t.Errorf("mudanças %v, esperado [[8 4]]", changes)
	}
}

func TestConfigZeradaUsaOPadrao(t *testing.T) {
	l := NewLimiter(LimiterConfig{})

	if got := l.Stats().Limit; got != 1 {
		t.Errorf("limite inicial %d, esperado 1", got)
	}
	acquire(t, l)(Timeout) // com beta inválido não pode travar nem zerar
	if got := l.Stats().Limit; got != 1 {
		t.Errorf("limite %d depois de um timeout no piso, esperado 1", got)
	}
}
