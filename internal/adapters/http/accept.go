// accept.go
// Handler de POST /instructions.
// Objetivo: traduzir a requisição HTTP em AcceptInstructionInput e o resultado em 202. A
// leitura do corpo é estrita (limite de tamanho, campo desconhecido, lixo depois do JSON e
// número não inteiro são recusados), porque este é o ponto onde dinheiro entra no sistema.
// A regra de negócio e a idempotência ficam no caso de uso, não aqui.

package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

// maxBodyBytes limita o corpo aceito: 1 MiB sobra para esta instrução.
const maxBodyBytes = 1 << 20

func (h *handler) postInstruction(w http.ResponseWriter, r *http.Request) {
	var req acceptRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	res, err := h.accept.Execute(r.Context(), acceptInput(r, req))
	if err != nil {
		writeError(w, r, err)
		return
	}

	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", "/instructions/"+string(res.ID))
	writeJSON(w, http.StatusAccepted, acceptResponse{ID: string(res.ID), Status: string(res.Status)})
}

// acceptInput monta o pedido do caso de uso. O scope vem do cliente autenticado, nunca do
// corpo: o cliente não escolhe em nome de quem a chave de idempotência vale.
func acceptInput(r *http.Request, req acceptRequest) app.AcceptInstructionInput {
	return app.AcceptInstructionInput{
		Scope:          clientFrom(r.Context()),
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Debtor:         req.Debtor,
		Creditor:       req.Creditor,
		AmountCents:    req.AmountCents,
		EndToEndID:     req.EndToEndID,
	}
}

// decodeJSON lê exatamente um objeto JSON do corpo para dst, de forma estrita.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return errBodyTooLarge
		}
		return errInvalidJSON
	}
	// Depois do objeto só pode haver o fim do corpo; qualquer coisa a mais é recusada.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return errBodyTooLarge
		}
		return errInvalidJSON
	}
	return nil
}
