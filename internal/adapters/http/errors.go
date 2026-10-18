// errors.go
// Tradução de erros do domínio e da aplicação para status HTTP e código de erro.
// Objetivo: um único lugar decide "este erro vira qual status". É uma tabela, então cada
// erro novo é uma linha e o teste percorre a tabela inteira. Erro desconhecido vira 500
// genérico: a mensagem interna nunca chega ao cliente.

package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// Erros que só existem na borda HTTP.
var (
	errUnauthorized = errors.New("token ausente, inválido ou expirado")
	errInvalidJSON  = errors.New("corpo da requisição não é um JSON válido para este endpoint")
	errBodyTooLarge = errors.New("corpo da requisição grande demais")
	errInvalidID    = errors.New("o id da instrução deve ser um UUID")
)

type errorMapping struct {
	target  error
	status  int
	code    string
	message string
}

// errorTable mapeia cada erro conhecido. A ordem não importa: os alvos são distintos.
var errorTable = []errorMapping{
	{errUnauthorized, http.StatusUnauthorized, "unauthorized", "token ausente, inválido ou expirado"},
	{app.ErrInvalidCredentials, http.StatusUnauthorized, "invalid_credentials", "client_id ou client_secret inválidos"},
	{errInvalidJSON, http.StatusBadRequest, "invalid_json", "corpo da requisição não é um JSON válido para este endpoint"},
	{errBodyTooLarge, http.StatusRequestEntityTooLarge, "body_too_large", "corpo da requisição grande demais"},
	{errInvalidID, http.StatusBadRequest, "invalid_id", "o id da instrução deve ser um UUID"},
	{app.ErrIdempotencyKeyRequired, http.StatusBadRequest, "idempotency_key_required", "o header Idempotency-Key é obrigatório"},
	{domain.ErrInvalidAmount, http.StatusUnprocessableEntity, "invalid_amount", "amount_cents deve ser maior que zero"},
	{domain.ErrInvalidParties, http.StatusUnprocessableEntity, "invalid_parties", "debtor, creditor e end_to_end_id são obrigatórios e debtor deve ser diferente de creditor"},
	{app.ErrIdempotencyKeyReuse, http.StatusUnprocessableEntity, "idempotency_key_reuse", "esta Idempotency-Key já foi usada com outro corpo"},
	{app.ErrNotFound, http.StatusNotFound, "not_found", "instrução não encontrada"},
	{app.ErrConcurrencyConflict, http.StatusConflict, "concurrency_conflict", "outra operação alterou a instrução ao mesmo tempo, tente novamente"},
}

// writeError escreve o erro no formato padrão. A mensagem vem da tabela, nunca do erro
// original, para não vazar detalhe interno.
func writeError(w http.ResponseWriter, err error) {
	for _, m := range errorTable {
		if errors.Is(err, m.target) {
			if errors.Is(m.target, errUnauthorized) {
				w.Header().Set("WWW-Authenticate", "Bearer")
			}
			writeJSON(w, m.status, errorBody{Error: errorDetail{Code: m.code, Message: m.message}})
			return
		}
	}
	// Erro desconhecido: registra o detalhe aqui e devolve só um 500 genérico.
	// Substituído por slog com correlation ID na sessão 7.
	log.Printf("erro interno: %v", err)
	writeJSON(w, http.StatusInternalServerError,
		errorBody{Error: errorDetail{Code: "internal", Message: "erro interno"}})
}

// writeJSON serializa v com o status dado.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("escrevendo resposta: %v", err)
	}
}
