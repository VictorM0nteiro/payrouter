// get.go
// Handler de GET /instructions/{id}.
// Objetivo: devolver o estado AUTORITATIVO da instrução (reconstruído do event store pelo
// caso de uso), de modo que quem acabou de criar uma instrução a veja na hora. O id é
// validado como UUID aqui na borda, senão o Postgres devolveria um erro de conversão que
// viraria um 500 em vez de um 400.

package httpapi

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// uuidTextLen é o tamanho do UUID canônico (8-4-4-4-12). uuid.Parse também aceita formas
// alternativas (chaves, urn:, sem hífens); a API aceita só a canônica.
const uuidTextLen = 36

func (h *handler) getInstruction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil || len(id) != uuidTextLen {
		writeError(w, errInvalidID)
		return
	}

	view, err := h.get.Execute(r.Context(), domain.InstructionID(id))
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, instructionResponse{
		ID:          string(view.ID),
		Status:      string(view.Status),
		Version:     view.Version,
		AmountCents: view.AmountCents,
		Debtor:      view.Debtor,
		Creditor:    view.Creditor,
		EndToEndID:  view.EndToEndID,
		Attempts:    view.Attempts,
	})
}
