// limiter_config_test.go
// Testes da leitura das variáveis do limite adaptativo e do adaptador concurrency, sem banco e
// sem Docker.
// Objetivo: provar que sem PARTICIPANT_LIMIT_MAX (ou com 0) o limite fica desligado, que os
// padrões são piso 1 e inicial = piso, que valores incoerentes ou inválidos impedem a partida em
// vez de cair num padrão em silêncio, e que o adaptador traduz cada resultado de envio para a
// regra AIMD certa.

package main

import (
	"context"
	"testing"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
)

// setLimitEnv define as quatro variáveis do limite; "" as deixa vazias (equivale a ausentes).
func setLimitEnv(t *testing.T, max, min, init string) {
	t.Helper()
	t.Setenv("PARTICIPANT_LIMIT_MAX", max)
	t.Setenv("PARTICIPANT_LIMIT_MIN", min)
	t.Setenv("PARTICIPANT_LIMIT_INIT", init)
}

func TestLimiteDesligadoSemMaxOuComMaxZero(t *testing.T) {
	for _, max := range []string{"", "0"} {
		setLimitEnv(t, max, "", "")

		cfg, err := limiterConfig()

		if err != nil || cfg.Max != 0 {
			t.Errorf("MAX=%q: cfg=%+v err=%v, esperado Max 0 (desligado) sem erro", max, cfg, err)
		}
	}
}

func TestLimiteLigadoUsaPisoUmEInicialIgualAoPiso(t *testing.T) {
	setLimitEnv(t, "8", "", "")

	cfg, err := limiterConfig()

	if err != nil || cfg.Max != 8 || cfg.Min != 1 || cfg.Initial != 1 {
		t.Errorf("cfg=%+v err=%v, esperado Max 8, Min 1, Initial 1", cfg, err)
	}
}

func TestLimiteLigadoLeOsTresValores(t *testing.T) {
	setLimitEnv(t, "16", "2", "5")

	cfg, err := limiterConfig()

	if err != nil || cfg.Max != 16 || cfg.Min != 2 || cfg.Initial != 5 {
		t.Errorf("cfg=%+v err=%v, esperado Max 16, Min 2, Initial 5", cfg, err)
	}
}

func TestLimiteIncoerenteOuInvalidoImpedeAPartida(t *testing.T) {
	cases := []struct{ name, max, min, init string }{
		{"piso acima do teto", "4", "6", ""},
		{"inicial acima do teto", "4", "1", "9"},
		{"inicial abaixo do piso", "8", "3", "2"},
		{"teto não numérico", "muitos", "", ""},
		{"teto negativo", "-1", "", ""},
		{"piso zero", "8", "0", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setLimitEnv(t, c.max, c.min, c.init)

			if cfg, err := limiterConfig(); err == nil {
				t.Errorf("cfg=%+v sem erro, esperado impedir a partida", cfg)
			}
		})
	}
}

func TestOutcomeOfTraduzCadaResultadoDeEnvio(t *testing.T) {
	cases := map[app.SendOutcome]elastic.Outcome{
		app.SendOK:        elastic.Success,
		app.SendThrottled: elastic.Throttled,
		app.SendUnknown:   elastic.Timeout,
		app.SendUnused:    elastic.Unused,
	}
	for in, want := range cases {
		if got := outcomeOf(in); got != want {
			t.Errorf("outcomeOf(%v) = %v, esperado %v", in, got, want)
		}
	}
}

func TestAdaptadorLigaOCasoDeUsoAoLimitador(t *testing.T) {
	lim := elastic.NewLimiter(elastic.LimiterConfig{Min: 1, Max: 8, Initial: 8})
	c := concurrency{lim}

	release, err := c.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := lim.Stats().InFlight; got != 1 {
		t.Errorf("em voo = %d, esperado 1 depois do Acquire", got)
	}
	release(app.SendThrottled)

	if s := lim.Stats(); s.InFlight != 0 || s.Limit != 4 {
		t.Errorf("estado %+v, esperado 0 em voo e limite 4 (429 divide por dois)", s)
	}
}

func TestAdaptadorDevolveOErroDoContextoSemTomarVaga(t *testing.T) {
	lim := elastic.NewLimiter(elastic.LimiterConfig{Min: 1, Max: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	release, err := concurrency{lim}.Acquire(ctx)

	if err == nil || release != nil {
		t.Errorf("Acquire devolveu release=%v err=%v, esperado nil e o erro do contexto", release != nil, err)
	}
	if got := lim.Stats().InFlight; got != 0 {
		t.Errorf("em voo = %d, esperado 0: o erro não toma vaga", got)
	}
}
