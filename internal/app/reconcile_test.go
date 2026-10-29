// reconcile_test.go
// Testes do Backoff da conciliação, sem banco e sem rede.
// Objetivo: garantir que a espera dobra a cada tentativa, nunca passa do teto, nunca cai
// abaixo da metade (o jitter só espalha a outra metade) e que o sorteio é injetável, para o
// resto do sistema ser testável de forma determinística.

package app

import (
	"testing"
	"time"
)

func TestBackoffDobraAteOTeto(t *testing.T) {
	const base, max = 5 * time.Second, time.Minute
	tests := []struct {
		attempts int
		ceiling  time.Duration
	}{
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{4, 40 * time.Second},
		{5, time.Minute},
		{50, time.Minute}, // muito além do teto: sem estouro
	}
	for _, tc := range tests {
		// r = 0: o mínimo, metade do teto da tentativa.
		if got := Backoff(tc.attempts, base, max, 0); got != tc.ceiling/2 {
			t.Errorf("Backoff(%d, r=0) = %v, esperado %v", tc.attempts, got, tc.ceiling/2)
		}
		// r perto de 1: quase o teto da tentativa, nunca acima.
		if got := Backoff(tc.attempts, base, max, 0.999999); got > tc.ceiling || got < tc.ceiling*99/100 {
			t.Errorf("Backoff(%d, r~1) = %v, esperado perto de %v e sem passar dele", tc.attempts, got, tc.ceiling)
		}
	}
}

func TestBackoffComJitterFicaEntreAMetadeEOTeto(t *testing.T) {
	for attempts := 1; attempts <= 8; attempts++ {
		for _, r := range []float64{0, 0.25, 0.5, 0.75, 0.999} {
			got := Backoff(attempts, time.Second, 30*time.Second, r)
			ceiling := min(time.Second<<(attempts-1), 30*time.Second)
			if got < ceiling/2 || got > ceiling {
				t.Errorf("Backoff(%d, %v) = %v fora de [%v, %v]", attempts, r, got, ceiling/2, ceiling)
			}
		}
	}
}
