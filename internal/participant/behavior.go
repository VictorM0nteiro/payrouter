// behavior.go
// Os seis comportamentos do participante simulado e a regra que escolhe um deles para cada
// instrução.
// Objetivo: tornar o estado desconhecido reproduzível sob demanda. Um prefixo no
// end_to_end_id ("late:abc") força o comportamento daquela instrução; sem prefixo, vale o
// padrão do processo. Assim os testes das sessões 15 e 16 provocam exatamente o caso que
// querem, pela API real, sem endpoint extra nem sorteio.

package participant

import (
	"fmt"
	"strings"
)

// Behavior é como o participante reage a uma instrução.
type Behavior string

const (
	// Accept processa e responde "accepted" na hora.
	Accept Behavior = "accept"
	// Reject processa e responde "rejected" com um código, na hora.
	Reject Behavior = "reject"
	// Slow processa e responde "accepted" depois de SlowDelay, ainda dentro do prazo do cliente.
	Slow Behavior = "slow"
	// Drop perde a mensagem: não processa e nunca responde. Para o cliente é um timeout, e a
	// consulta de status não acha nada.
	Drop Behavior = "drop"
	// Late processa na hora, mas só responde depois de LateDelay, além do prazo do cliente.
	// É o caso cruel: o cliente dá timeout e a operação aconteceu.
	Late Behavior = "late"
	// Crash derruba o processo antes de processar. Quem reinicia é o orquestrador.
	Crash Behavior = "crash"
)

// All lista os comportamentos conhecidos, na ordem em que a documentação os apresenta.
var All = []Behavior{Accept, Reject, Slow, Drop, Late, Crash}

// Parse reconhece o nome de um comportamento, sem diferenciar maiúsculas de minúsculas.
func Parse(s string) (Behavior, error) {
	name := Behavior(strings.ToLower(strings.TrimSpace(s)))
	for _, b := range All {
		if b == name {
			return b, nil
		}
	}
	return "", fmt.Errorf("comportamento desconhecido %q (válidos: %v)", s, All)
}

// Resolve escolhe o comportamento de uma instrução: o prefixo do end_to_end_id, se for o nome
// de um comportamento, ou def. Um prefixo desconhecido não é erro: cai no padrão, porque o
// end_to_end_id é um identificador do cliente e pode legitimamente conter dois-pontos.
func Resolve(endToEndID string, def Behavior) Behavior {
	prefix, _, found := strings.Cut(endToEndID, ":")
	if !found {
		return def
	}
	if b, err := Parse(prefix); err == nil {
		return b
	}
	return def
}
