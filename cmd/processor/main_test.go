// main_test.go
// Testes da leitura de configuração do processo processor e de um helper de teste.
// Objetivo: garantir que uma duração inválida, zero ou negativa impede a partida em vez de
// virar um padrão em silêncio, e que as listas de brokers ignoram espaços e itens vazios.

package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"
)

func decodeJSON(resp *http.Response, v any) error {
	return json.NewDecoder(resp.Body).Decode(v)
}

func TestDurationFromEnv(t *testing.T) {
	tests := []struct {
		name, value string
		want        time.Duration
		wantErr     bool
	}{
		{"vazia_usa_o_padrao", "", 3 * time.Second, false},
		{"valida", "250ms", 250 * time.Millisecond, false},
		{"invalida", "abc", 0, true},
		{"zero", "0s", 0, true},
		{"negativa", "-2s", 0, true},
		{"sem_unidade", "10", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PARTICIPANT_TIMEOUT", tc.value)

			got, err := durationFromEnv("PARTICIPANT_TIMEOUT", 3*time.Second)

			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("got %v, %v; esperado %v (erro=%v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestSplitList(t *testing.T) {
	got := splitList(" a:9092, ,b:9092,")
	if !slices.Equal(got, []string{"a:9092", "b:9092"}) {
		t.Errorf("splitList = %q", got)
	}
}
