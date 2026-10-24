// server.go
// O participante simulado: um servidor HTTP com estado em memória que aceita instruções de
// pagamento e as responde conforme o comportamento escolhido para cada uma.
// Objetivo: ser a contraparte externa do sistema, não um mock. Guarda o que processou, não
// processa a mesma instrução duas vezes (deduplica por instruction_id, como um participante
// real) e conta quantas vezes cada instrução chegou, para os testes provarem que nada foi
// enviado em duplicidade. A consulta de status (GET /payments/{id}) é a operação que a
// conciliação usa depois de um timeout.
package participant

import "time"

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
