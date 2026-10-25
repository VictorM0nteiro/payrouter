// server.go
// O participante simulado: um servidor HTTP com estado em memória que aceita instruções de
// pagamento e as responde conforme o comportamento escolhido para cada uma.
// Objetivo: ser a contraparte externa do sistema, não um mock. Guarda o que processou, não
// processa a mesma instrução duas vezes (deduplica por instruction_id, como um participante
// real) e conta quantas vezes cada instrução chegou, para os testes provarem que nada foi
// enviado em duplicidade. A consulta de status (GET /payments/{id}) é a operação que a
// conciliação usa depois de um timeout.
package participant

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	defaultSlowDelay  = time.Second
	defaultLateDelay  = 10 * time.Second
	defaultRejectCode = "insufficient_funds"

	// maxBodyBytes limita o corpo de uma instrução; um pagamento é minúsculo.
	maxBodyBytes = 1 << 20
)

// Config parametriza o participante. Campos zerados usam o padrão.
type Config struct {
	Default    Behavior      // comportamento sem marcador no end_to_end_id; padrão Accept
	SlowDelay  time.Duration // atraso do Slow
	LateDelay  time.Duration // atraso do Late
	RejectCode string        // código devolvido pelo Reject
	Exit       func(int)     // como o Crash derruba o processo; padrão os.Exit, injetável nos testes
}

// Request é a instrução recebida. O valor é sempre int64 de centavos.
type Request struct {
	InstructionID string `json:"instruction_id"`
	AmountCents   int64  `json:"amount_cents"`
	Debtor        string `json:"debtor"`
	Creditor      string `json:"creditor"`
	EndToEndID    string `json:"end_to_end_id"`
}

// result é o que o participante decidiu sobre uma instrução processada.
type result struct {
	Status string `json:"status"`
	Code   string `json:"code,omitempty"`
}

// statusBody é o corpo do GET /payments/{id}.
type statusBody struct {
	Status   string `json:"status"`
	Code     string `json:"code,omitempty"`
	Received int    `json:"received"`
}

// Server implementa http.Handler.
type Server struct {
	cfg       Config
	mux       *http.ServeMux
	mu        sync.Mutex
	processed map[string]result // instruções efetivamente processadas, por instruction_id
	received  map[string]int    // quantas vezes cada instrução chegou, processada ou não
}

func New(cfg Config) *Server {
	if cfg.Default == "" {
		cfg.Default = Accept
	}
	if cfg.SlowDelay <= 0 {
		cfg.SlowDelay = defaultSlowDelay
	}
	if cfg.LateDelay <= 0 {
		cfg.LateDelay = defaultLateDelay
	}
	if cfg.RejectCode == "" {
		cfg.RejectCode = defaultRejectCode
	}
	if cfg.Exit == nil {
		cfg.Exit = os.Exit
	}
	s := &Server{
		cfg:       cfg,
		mux:       http.NewServeMux(),
		processed: make(map[string]result),
		received:  make(map[string]int),
	}
	s.mux.HandleFunc("POST /payments", s.handlePay)
	s.mux.HandleFunc("GET /payments/{id}", s.handleStatus)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handlePay(w http.ResponseWriter, r *http.Request) {
	var req Request
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "corpo JSON inválido")
		return
	}
	if msg := validate(req); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}

	behavior := Resolve(req.EndToEndID, s.cfg.Default)
	s.countReceived(req.InstructionID)
	slog.InfoContext(r.Context(), "instrução recebida", "instruction_id", req.InstructionID, "behavior", string(behavior))

	switch behavior {
	case Crash:
		// Cai antes de tocar no estado: a instrução não foi processada.
		s.cfg.Exit(1)
		return
	case Drop:
		// A mensagem se perdeu: nada é gravado e nada é respondido. Segura a conexão até o
		// cliente desistir (ou o servidor encerrar).
		<-r.Context().Done()
		return
	}

	res := s.process(req.InstructionID, behavior)

	// Slow e Late diferem só no tamanho do atraso. No Late a operação já está gravada acima:
	// quando o cliente desiste e consulta o status, ela está lá.
	switch behavior {
	case Slow:
		if !wait(r.Context(), s.cfg.SlowDelay) {
			return
		}
	case Late:
		if !wait(r.Context(), s.cfg.LateDelay) {
			return
		}
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	s.mu.Lock()
	res, ok := s.processed[id]
	received := s.received[id]
	s.mu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, statusBody{Status: "not_found", Received: received})
		return
	}
	writeJSON(w, http.StatusOK, statusBody{Status: res.Status, Code: res.Code, Received: received})
}

// countReceived registra que a instrução chegou, mesmo que depois seja perdida ou derrube o processo.
func (s *Server) countReceived(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received[id]++
}

// process grava o resultado da instrução, uma única vez. Uma instrução já processada devolve o
// resultado guardado, sem reprocessar, qualquer que seja o comportamento da nova chegada.
func (s *Server) process(id string, b Behavior) result {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, ok := s.processed[id]; ok {
		return res
	}
	res := result{Status: "accepted"}
	if b == Reject {
		res = result{Status: "rejected", Code: s.cfg.RejectCode}
	}
	s.processed[id] = res
	return res
}

// validate devolve a mensagem do primeiro problema da instrução, ou "" se estiver válida.
func validate(r Request) string {
	switch {
	case r.InstructionID == "":
		return "instruction_id é obrigatório"
	case r.EndToEndID == "":
		return "end_to_end_id é obrigatório"
	case r.Debtor == "" || r.Creditor == "":
		return "debtor e creditor são obrigatórios"
	case r.AmountCents <= 0:
		return "amount_cents deve ser um inteiro positivo, em centavos"
	}
	return ""
}

// wait espera d ou até o contexto terminar; devolve false se o contexto terminou antes.
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		slog.Error("escrevendo resposta", "error", err)
	}
}

// writeError usa o mesmo formato de erro da API: {"error":{"code":"...","message":"..."}}.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
