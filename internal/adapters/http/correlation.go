// correlation.go
// Escolha do correlation ID de uma requisição.
// Objetivo: amarrar a requisição, o log de acesso e todo log emitido durante ela por um único
// id, que o cliente também recebe de volta no header. Se o cliente mandou um id utilizável
// (para rastrear de ponta a ponta), ele é reaproveitado; senão o servidor gera um UUID. O id
// do cliente só é aceito se for simples (até 64 caracteres de [A-Za-z0-9._-]), porque ele vai
// parar em log e em header: espaço, quebra de linha ou id gigante seriam injeção de log.

package httpapi

import (
	"net/http"

	"github.com/google/uuid"
)

const (
	correlationHeader = "X-Correlation-ID"
	maxCorrelationLen = 64
)

// validCorrelationID diz se o id enviado pelo cliente pode ser reaproveitado.
func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationLen {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// correlationIDFor devolve o id da requisição: o do cliente, se válido, ou um UUID novo.
func correlationIDFor(r *http.Request) string {
	if id := r.Header.Get(correlationHeader); validCorrelationID(id) {
		return id
	}
	return uuid.NewString()
}
