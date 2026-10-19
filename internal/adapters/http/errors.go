// errors.go
// Tradução de erros do domínio e da aplicação para status HTTP e código de erro.
// Objetivo: um único lugar decide "este erro vira qual status". É uma tabela, então cada
// erro novo é uma linha e o teste percorre a tabela inteira. Erro desconhecido vira 500
// genérico: a mensagem interna nunca chega ao cliente.

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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

	// Prazo estourado: o servidor não sabe se a operação chegou a ser gravada. É o estado
	// desconhecido na borda HTTP. Repetir com a mesma Idempotency-Key é seguro.
	{context.DeadlineExceeded, http.StatusGatewayTimeout, "request_timeout", "tempo esgotado: o resultado é desconhecido; repita com a mesma Idempotency-Key"},
	{context.Canceled, statusClientClosed, "request_canceled", "requisição cancelada pelo cliente"},
}

// statusClientClosed é o 499 (não padronizado, convenção do nginx): o cliente desistiu antes
// da resposta.
const statusClientClosed = 499

// writeError escreve o erro no formato padrão. A mensagem vem da tabela, nunca do erro
// original, para não vazar detalhe interno.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	for _, m := range errorTable {
		if errors.Is(err, m.target) {
			if errors.Is(m.target, errUnauthorized) {
				w.Header().Set("WWW-Authenticate", "Bearer")
			}
			writeJSON(w, m.status, errorBody{Error: errorDetail{Code: m.code, Message: m.message}})
			return
		}
	}
	// Erro desconhecido: o detalhe vai para o log (com o correlation ID do contexto) e o
	// cliente recebe só um 500 genérico.
	slog.ErrorContext(r.Context(), "erro interno", "error", err)
	writeJSON(w, http.StatusInternalServerError,
		errorBody{Error: errorDetail{Code: "internal", Message: "erro interno"}})
}

// writeJSON serializa v com o status dado.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("escrevendo resposta", "error", err)
	}
}
