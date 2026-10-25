// main_test.go
// Testes da leitura de configuração do processo participant.
// Objetivo: garantir que uma duração inválida, zero ou negativa impede a partida em vez de
// virar um padrão em silêncio, e que a variável vazia usa o padrão.

package main

import (
	"testing"
	"time"
)

func TestDurationFromEnv(t *testing.T) {
	tests := []struct {
		name, value string
		want        time.Duration
		wantErr     bool
	}{
		{"vazia_usa_o_padrao", "", time.Second, false},
		{"valida", "250ms", 250 * time.Millisecond, false},
		{"invalida", "abc", 0, true},
		{"zero", "0s", 0, true},
		{"negativa", "-2s", 0, true},
		{"sem_unidade", "10", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SLOW_DELAY", tc.value)

			got, err := durationFromEnv("SLOW_DELAY", time.Second)

			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("got %v, %v; esperado %v (erro=%v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
