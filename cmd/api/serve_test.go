// serve_test.go
// Testes do desligamento elegante da API, sem banco e sem Docker.
// Objetivo: provar as três promessas do desligamento: nenhuma conexão nova depois do sinal,
// a requisição que já estava em andamento termina com sucesso, e uma requisição que não
// termina no prazo é cortada em vez de segurar o processo para sempre.

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

// startServer sobe um servidor cujo único handler avisa que começou e espera release.
func startServer(t *testing.T, shutdownTimeout time.Duration) (url string, started, release chan struct{}, cancel context.CancelFunc, done chan error) {
	t.Helper()
	started, release = make(chan struct{}), make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	})}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done = make(chan error, 1)
	go func() { done <- serve(ctx, srv, ln, shutdownTimeout) }()
	return "http://" + ln.Addr().String(), started, release, cancel, done
}

// get dispara uma requisição e entrega o resultado em um canal.
func get(url string) chan error {
	res := make(chan error, 1)
	go func() {
		resp, err := http.Get(url) //nolint:noctx // teste
		if err == nil {
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				err = errors.New(resp.Status)
			}
		}
		res <- err
	}()
	return res
}

func TestShutdownTerminaARequisicaoEmAndamentoERecusaNovas(t *testing.T) {
	url, started, release, cancel, done := startServer(t, 5*time.Second)
	inflight := get(url)
	<-started // a requisição já está dentro do handler

	cancel() // chega o SIGTERM

	// O processo ainda não terminou: está esperando a requisição.
	select {
	case err := <-done:
		t.Fatalf("serve voltou com a requisição em andamento: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Nenhuma conexão nova é aceita depois do sinal.
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", url[len("http://"):], 100*time.Millisecond)
		if err != nil {
			break
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("o listener continuou aceitando conexões depois do sinal")
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(release) // o handler termina

	if err := <-inflight; err != nil {
		t.Errorf("a requisição em andamento deveria terminar com 200: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve = %v, esperado nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve não voltou depois da requisição terminar")
	}
}

func TestShutdownCortaARequisicaoQueNaoTerminaNoPrazo(t *testing.T) {
	url, started, release, cancel, done := startServer(t, 100*time.Millisecond)
	defer close(release)
	inflight := get(url)
	<-started

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("serve = %v, esperado DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("o desligamento deveria desistir depois do prazo")
	}
	if err := <-inflight; err == nil {
		t.Error("a requisição cortada deveria falhar para o cliente")
	}
}

func TestServeSemSinalDevolveOErroDoServidor(t *testing.T) {
	srv := &http.Server{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close() // listener já fechado: Serve falha de imediato

	err = serve(context.Background(), srv, ln, time.Second)

	if err == nil {
		t.Error("esperado erro do servidor")
	}
}

func TestDurationFromEnv(t *testing.T) {
	tests := []struct {
		name, value string
		want        time.Duration
		wantErr     bool
	}{
		{"vazia_usa_o_padrao", "", 5 * time.Second, false},
		{"valida", "2s", 2 * time.Second, false},
		{"invalida", "abc", 0, true},
		{"zero", "0s", 0, true},
		{"negativa", "-1s", 0, true},
		{"sem_unidade", "5", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("REQUEST_TIMEOUT", tc.value)

			got, err := durationFromEnv("REQUEST_TIMEOUT", 5*time.Second)

			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("got %v, %v; esperado %v (erro=%v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
