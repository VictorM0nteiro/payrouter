// concurrency_test.go
// Testes do limite adaptativo de requisições em voo no caso de uso, contra Postgres real.
// Objetivo: provar a sessão 19a. A vaga é tomada ANTES de gravar qualquer coisa (um contexto vencido
// ali não deixa instrução presa), volta com o resultado certo do envio (ok, 429, desconhecido, não
// usada), nunca há mais envios simultâneos do que o limite, e um timeout derruba o limite sem
// reenviar nada.

package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
)

// fakeConcurrency anota as vagas tomadas e como cada uma voltou.
type fakeConcurrency struct {
	acquireErr error

	mu       sync.Mutex
	acquired int
	outcomes []app.SendOutcome
}

func (f *fakeConcurrency) Acquire(context.Context) (func(app.SendOutcome), error) {
	if f.acquireErr != nil {
		return nil, f.acquireErr
	}
	f.mu.Lock()
	f.acquired++
	f.mu.Unlock()
	return func(o app.SendOutcome) {
		f.mu.Lock()
		f.outcomes = append(f.outcomes, o)
		f.mu.Unlock()
	}, nil
}

func (f *fakeConcurrency) got() []app.SendOutcome {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]app.SendOutcome(nil), f.outcomes...)
}

func TestVagaEmVooVemAntesDaReivindicacao(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "cc-1")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}
	uc := newUseCase(fake).WithConcurrency(&fakeConcurrency{acquireErr: context.DeadlineExceeded})

	err := uc.Handle(context.Background(), acceptedMsg(id))

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("erro = %v, esperado o erro do contexto, repetível", err)
	}
	if errors.Is(err, app.ErrOutcomeUnknown) {
		t.Error("esperar a vaga não pode parecer um envio sem resposta")
	}
	if got := join(eventTypes(t, id)); got != "InstructionAccepted" {
		t.Errorf("eventos = %s, esperado só InstructionAccepted: esperar a vaga não grava nada", got)
	}
	if fake.calls.Load() != 0 {
		t.Errorf("envios = %d, esperado 0", fake.calls.Load())
	}
}

func TestVagaVoltaComOResultadoDoEnvio(t *testing.T) {
	requireInfra(t)
	cases := []struct {
		name string
		err  error
		res  app.PaymentResult
		want app.SendOutcome
	}{
		{"conclusivo", nil, app.PaymentResult{Status: app.PaymentAccepted}, app.SendOK},
		{"429", &app.ThrottledError{}, app.PaymentResult{}, app.SendThrottled},
		{"desconhecido", app.ErrOutcomeUnknown, app.PaymentResult{}, app.SendUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id := newAccepted(t, "cc-out-"+c.name)
			cc := &fakeConcurrency{}
			uc := newUseCase(&fakeParticipant{result: c.res, err: c.err}).WithConcurrency(cc)

			if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
				t.Fatal(err)
			}

			if got := cc.got(); len(got) != 1 || got[0] != c.want {
				t.Errorf("a vaga voltou com %v, esperado exatamente [%v]", got, c.want)
			}
		})
	}
}

func TestMensagemRepetidaDevolveAVagaSemUso(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "cc-dup")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}
	cc := &fakeConcurrency{}
	uc := newUseCase(fake).WithConcurrency(cc)

	for range 2 {
		if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
			t.Fatal(err)
		}
	}

	got := cc.got()
	if len(got) != 2 || got[0] != app.SendOK || got[1] != app.SendUnused {
		t.Errorf("vagas devolvidas %v, esperado [SendOK SendUnused]: a repetida não enviou nada", got)
	}
	if fake.calls.Load() != 1 {
		t.Errorf("envios = %d, esperado 1", fake.calls.Load())
	}
}

// peakParticipant mede quantos Pay rodam ao mesmo tempo.
type peakParticipant struct {
	fakeParticipant
	cur, peak atomic.Int32
}

func (p *peakParticipant) Pay(ctx context.Context, req app.PaymentRequest) (app.PaymentResult, error) {
	n := p.cur.Add(1)
	for {
		old := p.peak.Load()
		if n <= old || p.peak.CompareAndSwap(old, n) {
			break
		}
	}
	defer p.cur.Add(-1)
	return p.fakeParticipant.Pay(ctx, req)
}

func TestNuncaHaMaisEnviosSimultaneosDoQueOLimiteReal(t *testing.T) {
	requireInfra(t)
	const n, limit = 12, 3
	part := &peakParticipant{fakeParticipant: fakeParticipant{
		result: app.PaymentResult{Status: app.PaymentAccepted}, delay: 60 * time.Millisecond,
	}}
	uc := newUseCase(part).WithConcurrency(concurrency{elastic.NewLimiter(elastic.LimiterConfig{Min: limit, Max: limit})})
	ids := newBatch(t, "cc-peak", n)

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if got := part.peak.Load(); got > limit {
		t.Errorf("pico de %d envios simultâneos com limite %d", got, limit)
	}
	if got := part.peak.Load(); got < 2 {
		t.Errorf("pico de %d: o limite não deixou paralelizar", got)
	}
	for _, id := range ids {
		if got := load(t, id).Status(); got != domain.StatusSettled {
			t.Errorf("%s: status = %s, esperado settled", id, got)
		}
	}
	if part.calls.Load() != n {
		t.Errorf("envios = %d, esperado %d (um por instrução)", part.calls.Load(), n)
	}
}

func TestTimeoutDerrubaOLimiteENaoReenvia(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "cc-timeout")
	fake := &fakeParticipant{err: app.ErrOutcomeUnknown}
	lim := elastic.NewLimiter(elastic.LimiterConfig{Min: 1, Max: 8, Initial: 8})
	uc := newUseCase(fake).WithConcurrency(concurrency{lim})

	if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatal(err)
	}

	if got := lim.Stats().Limit; got != 4 {
		t.Errorf("limite = %d, esperado 4 (dividido pelo timeout)", got)
	}
	if got := load(t, id).Status(); got != domain.StatusUnknown {
		t.Errorf("status = %s, esperado unknown", got)
	}
	if fake.calls.Load() != 1 {
		t.Errorf("envios = %d, esperado 1: um timeout derruba o limite e nunca reenvia", fake.calls.Load())
	}
}

func TestOs429DerrubamOLimiteMasAInstrucaoVoltaAAccepted(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "cc-429")
	fake := &fakeParticipant{err: &app.ThrottledError{}}
	lim := elastic.NewLimiter(elastic.LimiterConfig{Min: 1, Max: 8, Initial: 8})
	uc := newUseCase(fake).WithConcurrency(concurrency{lim})

	if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatal(err)
	}

	if got := lim.Stats().Limit; got != 4 {
		t.Errorf("limite = %d, esperado 4 (dividido pelo 429)", got)
	}
	if got := load(t, id).Status(); got != domain.StatusAccepted {
		t.Errorf("status = %s, esperado accepted: 429 é não processado", got)
	}
}
