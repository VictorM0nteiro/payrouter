// dto.go
// Formatos de entrada e saída JSON da API.
// Objetivo: separar o contrato público do domínio. O nome dos campos e o tipo (valor sempre
// em amount_cents, inteiro) são decisão da borda; mudar o JSON nunca exige mexer no domínio,
// e o domínio nunca ganha tags de serialização.

package httpapi

// tokenRequest é o corpo de POST /auth/token.
type tokenRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

// tokenResponse é a resposta 200 de POST /auth/token. expires_in é em segundos.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

// acceptRequest é o corpo de POST /instructions.
type acceptRequest struct {
	Debtor      string `json:"debtor"`
	Creditor    string `json:"creditor"`
	AmountCents int64  `json:"amount_cents"`
	EndToEndID  string `json:"end_to_end_id"`
}

// acceptResponse é a resposta 202 de POST /instructions.
type acceptResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// instructionResponse é a resposta de GET /instructions/{id}.
type instructionResponse struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Version     int    `json:"version"`
	AmountCents int64  `json:"amount_cents"`
	Debtor      string `json:"debtor"`
	Creditor    string `json:"creditor"`
	EndToEndID  string `json:"end_to_end_id"`
	Attempts    int    `json:"attempts"`
}

// errorBody é o formato único de erro: um código estável para máquinas e uma mensagem para
// humanos.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
