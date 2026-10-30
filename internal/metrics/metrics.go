// metrics.go
// Métricas Prometheus do processor e do relay, portadas do jeito do wallet-go (registry próprio
// por binário, recorders que não fazem nada quando nulos). É reuso declarado, não contribuição
// deste projeto.
// Objetivo: tornar visível o que só existe porque a fronteira é assíncrona: 429 contra timeout
// lado a lado, a espera do limite de saída, o atraso do outbox e a fila de conciliação.
// Os labels são sempre conjuntos fechados; NUNCA aggregate_id ou instruction_id (cada valor novo
// criaria uma série nova para sempre). O domínio não importa este pacote.

package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "payrouter"

// NewRegistry cria um registry novo, nunca o global: cada binário (e cada teste) tem o seu, sem
// vazar contagem de um para o outro.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Handler serve o registry no formato de texto do Prometheus.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// Serve expõe GET /metrics em addr até ctx terminar. addr vazio desliga as métricas.
//
// shortcut: sem autenticação e sem TLS; a porta fica na rede do compose e presa a 127.0.0.1 no
// host. Antes de expor fora disso, pôr um proxy na frente.
func Serve(ctx context.Context, addr string, reg *prometheus.Registry) error {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Handler(reg))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = srv.Shutdown(shutdownCtx)
	}()
	err := srv.ListenAndServe()
	cancel() // se falhou por outro motivo (porta ocupada), solta a goroutine
	<-done   // ListenAndServe volta assim que o Shutdown começa; espera ele drenar de verdade
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Processor reúne o que o binário processor mede. Todos os métodos aceitam o receptor nulo e
// então não fazem nada, para o código e os testes que não ligam métricas seguirem como estão.
type Processor struct {
	calls            *prometheus.CounterVec
	callDuration     *prometheus.HistogramVec
	outcomes         *prometheus.CounterVec
	backpressure     prometheus.Counter
	backpressureWait prometheus.Counter
	reconciled       *prometheus.CounterVec
	records          *prometheus.CounterVec
	pauses           prometheus.Counter
}

// NewProcessor cria e registra as métricas do processor em reg.
func NewProcessor(reg *prometheus.Registry) *Processor {
	m := &Processor{
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "participant_calls_total",
			Help: "Chamadas ao participante, por operação (pay, status) e resultado (ok, throttled, unknown). 429 e timeout são séries diferentes.",
		}, []string{"op", "result"}),
		callDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "participant_call_duration_seconds",
			Help:    "Duração das chamadas ao participante, por operação.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "routing_outcomes_total",
			Help: "O que cada envio ao participante resultou: settled, rejected, throttled (429, será reenviado) ou unknown (sem resposta, só consulta).",
		}, []string{"outcome"}),
		backpressure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "output_backpressure_total",
			Help: "Vezes em que o limite de saída mandou esperar antes de enviar.",
		}),
		backpressureWait: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "output_backpressure_wait_seconds_total",
			Help: "Soma da espera mandada pelo limite de saída, em segundos.",
		}),
		reconciled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "reconciliation_queries_total",
			Help: "Consultas de conciliação, por resultado: resolved, inconclusive ou exhausted (desistiu, precisa de uma pessoa).",
		}, []string{"result"}),
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "consumer_records_total",
			Help: "Tentativas de tratar um registro do Kafka, por resultado: ok, unknown, backpressure, error ou invalid.",
		}, []string{"result"}),
		pauses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "consumer_partition_pauses_total",
			Help: "Vezes em que o consumidor pausou uma partição esperando o limite de saída.",
		}),
	}
	reg.MustRegister(m.calls, m.callDuration, m.outcomes, m.backpressure, m.backpressureWait, m.reconciled, m.records, m.pauses)
	return m
}

// Call registra uma chamada ao participante. op é "pay" ou "status"; result, "ok", "throttled"
// ou "unknown".
func (m *Processor) Call(op, result string, d time.Duration) {
	if m == nil {
		return
	}
	m.calls.WithLabelValues(op, result).Inc()
	m.callDuration.WithLabelValues(op).Observe(d.Seconds())
}

// Outcome registra o que o envio resultou: "settled", "rejected", "throttled" ou "unknown".
func (m *Processor) Outcome(outcome string) {
	if m == nil {
		return
	}
	m.outcomes.WithLabelValues(outcome).Inc()
}

// Backpressure registra uma espera mandada pelo limite de saída.
func (m *Processor) Backpressure(wait time.Duration) {
	if m == nil {
		return
	}
	m.backpressure.Inc()
	m.backpressureWait.Add(wait.Seconds())
}

// Reconciled registra uma consulta de conciliação: "resolved", "inconclusive" ou "exhausted".
func (m *Processor) Reconciled(result string) {
	if m == nil {
		return
	}
	m.reconciled.WithLabelValues(result).Inc()
}

// Record registra o resultado de uma tentativa de tratar um registro do Kafka.
func (m *Processor) Record(result string) {
	if m == nil {
		return
	}
	m.records.WithLabelValues(result).Inc()
}

// Pause registra uma partição pausada pelo limite de saída.
func (m *Processor) Pause() {
	if m == nil {
		return
	}
	m.pauses.Inc()
}

// Relay reúne o que o binário relay mede. Aceita o receptor nulo, como Processor.
type Relay struct {
	published prometheus.Counter
	errors    prometheus.Counter
	duration  prometheus.Histogram
}

// NewRelay cria e registra as métricas do relay em reg.
func NewRelay(reg *prometheus.Registry) *Relay {
	m := &Relay{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "relay_published_total",
			Help: "Linhas do outbox publicadas e marcadas. Entrega pelo menos uma vez: a mesma linha pode sair duas vezes se o relay cair entre publicar e marcar.",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "relay_round_errors_total",
			Help: "Rodadas do relay que falharam e serão repetidas.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "relay_round_duration_seconds",
			Help:    "Duração das rodadas que publicaram algo ou falharam. Rodadas vazias não entram.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.published, m.errors, m.duration)
	return m
}

// Round registra uma rodada: n linhas publicadas, ou err.
func (m *Relay) Round(n int, err error, d time.Duration) {
	if m == nil || (n == 0 && err == nil) {
		return
	}
	m.duration.Observe(d.Seconds())
	if err != nil {
		m.errors.Inc()
		return
	}
	m.published.Add(float64(n))
}

// Backlog é o que está esperando no banco neste instante.
type Backlog struct {
	ReconciliationTasks int64   // instruções unknown à espera de consulta
	OutboxPending       int64   // linhas do outbox ainda não publicadas
	OutboxOldestAge     float64 // idade, em segundos, da linha pendente mais antiga (0 se não há)
}

// RegisterBacklog registra as filas do banco como gauges lidos NA HORA da coleta, com uma única
// consulta por coleta. Se a consulta falha, a coleta omite as três séries em vez de mentir.
//
// shortcut: uma consulta por coleta (5 s). Barato neste tamanho; se pesar sob carga, trocar por
// um laço que atualiza de tempos em tempos (sessão 27).
func RegisterBacklog(reg *prometheus.Registry, read func(ctx context.Context) (Backlog, error)) {
	reg.MustRegister(&backlogCollector{
		read: read,
		tasks: prometheus.NewDesc(namespace+"_reconciliation_tasks_pending",
			"Tarefas de conciliação na fila (instruções unknown à espera de consulta).", nil, nil),
		pending: prometheus.NewDesc(namespace+"_outbox_pending",
			"Linhas do outbox ainda não publicadas.", nil, nil),
		age: prometheus.NewDesc(namespace+"_outbox_oldest_pending_age_seconds",
			"Idade da linha pendente mais antiga do outbox; 0 quando não há pendência.", nil, nil),
	})
}

type backlogCollector struct {
	read    func(ctx context.Context) (Backlog, error)
	tasks   *prometheus.Desc
	pending *prometheus.Desc
	age     *prometheus.Desc
}

func (c *backlogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasks
	ch <- c.pending
	ch <- c.age
}

func (c *backlogCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	b, err := c.read(ctx)
	if err != nil {
		slog.Warn("métricas: consulta das filas falhou, séries omitidas", "error", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(b.ReconciliationTasks))
	ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(b.OutboxPending))
	ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, b.OutboxOldestAge)
}

type HTTP struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewHTTP(reg *prometheus.Registry) *HTTP {
	m := &HTTP{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total",
			Help: "Requisições da API, por rota (o padrão do mux, nunca o caminho) e status. 429 rate_limited e 504 (resultado desconhecido para o cliente) aparecem lado a lado.",
		}, []string{"route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help:    "Duração das requisições da API, por rota.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// Request registra uma requisição atendida. route é o padrão do mux ("POST /instructions") ou
// "unmatched"; sem label de método, porque numa rota inexistente ele vem do cliente e não tem teto.
func (m *HTTP) Request(route string, status int, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(route, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(route).Observe(d.Seconds())
}

// Participant reúne o que o participante simulado mede: a verdade do outro lado da fronteira,
// para comparar com o que o processor enxerga. Aceita o receptor nulo, como Processor.
type Participant struct {
	payments  *prometheus.CounterVec
	throttled prometheus.Counter
}

func NewParticipant(reg *prometheus.Registry) *Participant {
	m := &Participant{
		payments: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "participant_payments_total",
			Help: "Instruções que o participante tratou, por comportamento e resultado: accepted, rejected, dropped (perdida, sem resposta) ou crashed (caiu antes de processar).",
		}, []string{"behavior", "result"}),
		throttled: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "participant_throttled_total",
			Help: "Chegadas recusadas com 429 pelo participante: não processadas, reenviar é seguro.",
		}),
	}
	reg.MustRegister(m.payments, m.throttled)
	return m
}

func (m *Participant) Payment(behavior, result string) {
	if m == nil {
		return
	}
	m.payments.WithLabelValues(behavior, result).Inc()
}

func (m *Participant) Throttled() {
	if m == nil {
		return
	}
	m.throttled.Inc()
}
