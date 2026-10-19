// token.go
// Handler de POST /auth/token.
// Objetivo: trocar client_id + client_secret por um token temporário. É a única rota de
// instrução-adjacente sem autenticação prévia. A resposta não diz se o cliente existe: id
// desconhecido, segredo errado e cliente inativo devolvem o mesmo 401. O token nunca pode
// ser guardado em cache por intermediários (Cache-Control: no-store).

package httpapi

import (
	"net/http"
)

func (h *handler) postToken(w http.ResponseWriter, r *http.Request) {
	var req tokenRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	res, err := h.authn.Execute(r.Context(), req.ClientID, req.ClientSecret)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, tokenResponse{
		AccessToken: res.AccessToken,
		TokenType:   "Bearer",
		ExpiresIn:   int64(res.ExpiresIn.Seconds()),
	})
}
