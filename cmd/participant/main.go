// main.go
// Ponto de entrada do processo participant: lê a configuração do ambiente (ADDR,
// PARTICIPANT_BEHAVIOR, SLOW_DELAY, LATE_DELAY, REJECT_CODE, PARTICIPANT_TPS, PARTICIPANT_BURST,
// LOG_FORMAT) e sobe o participante
// simulado.
// Objetivo: dar ao sistema uma contraparte externa configurável, que pode aceitar, rejeitar,
// demorar, perder a mensagem, responder depois do timeout ou cair. O valor inválido de qualquer
// variável impede a partida, em vez de cair num padrão em silêncio.

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/logging"
	"github.com/VictorM0nteiro/payrouter/internal/participant"
)

const (
	defaultAddr = ":9090"

	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 5 * time.Second

	// writeSlack é a folga somada ao LATE_DELAY no WriteTimeout. O WriteTimeout precisa ser
	// maior que o atraso, senão a resposta tardia nunca seria escrita.
	writeSlack = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "participant:", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(logging.New(os.Getenv("LOG_FORMAT"), os.Stdout))

	behavior := participant.Accept
	if raw := os.Getenv("PARTICIPANT_BEHAVIOR"); raw != "" {
		b, err := participant.Parse(raw)
		if err != nil {
			return fmt.Errorf("PARTICIPANT_BEHAVIOR: %w", err)
		}
		behavior = b
	}
	slowDelay, err := durationFromEnv("SLOW_DELAY", time.Second)
	if err != nil {
		return err
	}
	lateDelay, err := durationFromEnv("LATE_DELAY", 10*time.Second)
	if err != nil {
		return err
	}
	tps, err := floatFromEnv("PARTICIPANT_TPS", 0)
	if err != nil {
		return err
	}
	burst, err := intFromEnv("PARTICIPANT_BURST", 0)
	if err != nil {
		return err
	}
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	handler := participant.New(participant.Config{
		Default:    behavior,
		SlowDelay:  slowDelay,
		LateDelay:  lateDelay,
		RejectCode: os.Getenv("REJECT_CODE"),
		TPS:        tps,
		Burst:      burst,
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      lateDelay + writeSlack,
		IdleTimeout:       idleTimeout,
	}
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("participante ouvindo", "addr", addr, "behavior", string(behavior),
		"slow_delay", slowDelay.String(), "late_delay", lateDelay.String(), "tps", tps, "burst", burst)

	select {
	case err := <-errc:
		return err // não subiu (porta ocupada, por exemplo) ou caiu sozinho
	case <-sigCtx.Done():
	}

	// Requisições presas (drop, late) não terminam sozinhas: Shutdown espera até o prazo e
	// Close corta o que sobrar.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	slog.Info("participante encerrado")
	return nil
}

// durationFromEnv lê uma duração (ex.: "2s") da variável, ou devolve def se ela estiver vazia.
func durationFromEnv(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: %q não é uma duração positiva (ex.: 2s)", name, raw)
	}
	return d, nil
}

// floatFromEnv lê um número não negativo da variável, ou devolve def se ela estiver vazia. Zero
// desliga o limite.
func floatFromEnv(name string, def float64) (float64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("%s: %q não é um número não negativo", name, raw)
	}
	return f, nil
}

// intFromEnv lê um inteiro não negativo da variável, ou devolve def se ela estiver vazia.
func intFromEnv(name string, def int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: %q não é um inteiro não negativo", name, raw)
	}
	return n, nil
}
