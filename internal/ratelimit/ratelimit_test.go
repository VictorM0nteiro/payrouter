// ratelimit_test.go
// Testes do balde de fichas, sem rede e sem banco.
// Objetivo: provar a rajada, o atraso informado quando as fichas acabam, a recuperação com o
// tempo e, sobretudo, que uma reserva recusada NÃO consome ficha (senão a mensagem repetida pelo
// consumidor pagaria duas vezes e o limite efetivo cairia pela metade).

package ratelimit

import (
	"testing"
	"time"
)

func TestRajadaPassaEDepoisManda(t *testing.T) {
	b := New(10, 3)

	for i := range 3 {
		if d := b.Reserve(); d != 0 {
			t.Fatalf("reserva %d da rajada esperou %v, esperado 0", i, d)
		}
	}
	d := b.Reserve()
	if d <= 0 || d > 150*time.Millisecond {
		t.Errorf("4ª reserva mandou esperar %v, esperado algo entre 0 e 100ms (10/s)", d)
	}
}

func TestEsperarERepetirNaoCobraDuasFichas(t *testing.T) {
	b := New(20, 1) // uma ficha a cada 50ms
	if d := b.Reserve(); d != 0 {
		t.Fatalf("1ª reserva = %v, esperado 0", d)
	}

	// Pede várias vezes enquanto está sem ficha: nenhuma recusa pode consumir a ficha futura.
	var first time.Duration
	for i := range 5 {
		d := b.Reserve()
		if d <= 0 {
			t.Fatalf("reserva %d = %v, esperado precisar esperar", i, d)
		}
		if i == 0 {
			first = d
		}
	}
	time.Sleep(first + 10*time.Millisecond)

	if d := b.Reserve(); d != 0 {
		t.Errorf("depois de esperar %v a reserva mandou esperar %v, esperado 0", first, d)
	}
}

func TestRecuperaComOTempo(t *testing.T) {
	b := New(100, 1)
	b.Reserve()
	if d := b.Reserve(); d <= 0 {
		t.Fatalf("esperado precisar esperar, veio %v", d)
	}
	time.Sleep(30 * time.Millisecond) // 100/s: 10ms por ficha

	if d := b.Reserve(); d != 0 {
		t.Errorf("reserva depois de recuperar = %v, esperado 0", d)
	}
}

func TestBurstMaiorQueOBaldeNaoRecusaParaSempre(t *testing.T) {
	// rate.Limiter com burst 0 nunca concede ficha (OK() falso): deve deixar passar.
	b := New(10, 0)

	if d := b.Reserve(); d != 0 {
		t.Errorf("reserva com burst 0 = %v, esperado 0 (falha aberta)", d)
	}
}
