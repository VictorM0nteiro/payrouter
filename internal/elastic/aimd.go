// aimd.go
// A regra AIMD (aumento aditivo, diminuição multiplicativa) do limite de requisições em voo,
// como função pura.
// Objetivo: separar a decisão (testável sem goroutines nem relógio) da mecânica de segurar vagas
// (limiter.go). Um sucesso soma 1/limite, o que dá cerca de +1 por janela de limite respostas; um
// 429 ou um timeout multiplica o limite por Beta. Cair rápido e subir devagar é o que deixa o
// limite achar a capacidade de um participante que não a informa, sem martelá-lo.

package elastic

// Outcome é o que aconteceu com uma requisição liberada de volta ao limitador.
type Outcome int

const (
	// Success é uma resposta conclusiva do participante.
	Success Outcome = iota
	// Throttled é um 429: o participante recusou, nada foi processado.
	Throttled
	// Timeout é um resultado desconhecido (sem resposta, erro de rede, 5xx). Reduz o limite como
	// controle de carga; NUNCA autoriza reenviar.
	Timeout
	// Unused devolve a vaga sem que nenhuma requisição tenha saído (a mensagem era repetida, outro
	// processor reivindicou antes). Não move o limite.
	Unused
)

// aimd guarda os parâmetros da regra.
type aimd struct {
	min, max float64
	beta     float64
}

// next devolve o limite depois de o.
func (a aimd) next(limit float64, o Outcome) float64 {
	if o == Success {
		return min(a.max, limit+1/limit)
	}
	return max(a.min, limit*a.beta)
}
