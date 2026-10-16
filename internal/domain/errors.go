// Erros tipados do domínio.
// Objetivo: transição ilegal e dado inválido são erros do domínio, identificáveis com
// errors.Is/errors.As pelas camadas de cima (a API mapeia para status HTTP). Nada disso é
// decidido por constraint de banco nem por `if` espalhado em handler.

package domain

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidAmount        = errors.New("valor deve ser maior que zero")
	ErrInvalidParties       = errors.New("id, devedor, credor e end-to-end id são obrigatórios e devedor deve diferir do credor")
	ErrInvalidQueryResult   = errors.New("resultado de consulta de status inválido")
	ErrInvalidRejectionCode = errors.New("código de rejeição é obrigatório")
	ErrEmptyHistory         = errors.New("histórico de eventos vazio")
)

// ErrIllegalTransition indica que o evento não é permitido no estado atual. Carrega os
// dois lados para o erro ser diagnosticável sem olhar o código.
type ErrIllegalTransition struct {
	From  Status
	Event EventType
}

func (e ErrIllegalTransition) Error() string {
	return fmt.Sprintf("transição ilegal: evento %s não é permitido no estado %q", e.Event, e.From)
}

//incompleto