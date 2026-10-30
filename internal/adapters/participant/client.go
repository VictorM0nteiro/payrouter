// client.go
// Adapter HTTP da porta app.Participant: envia a instrução ao participante (POST /payments) e
// consulta o status dela (GET /payments/{id}).
// Objetivo: traduzir a resposta HTTP na distinção que importa. 200 com accepted/rejected é
// conclusivo; QUALQUER outra coisa (timeout, conexão caída, 5xx, corpo ilegível, até um 429
// por enquanto) vira app.ErrOutcomeUnknown, porque a requisição pode ter chegado e ter sido
// processada. O prazo do cliente é próprio e menor que o do participante "late", para o caso
// cruel acontecer de verdade.

package participant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

// maxResponseBytes limita a leitura da resposta; ela é minúscula.
const maxResponseBytes = 1 << 20

// Client implementa app.Participant.
type Client struct {
	baseURL string
	http    *http.Client
}

var _ app.Participant = (*Client)(nil)

// NewClient aponta para o participante (ex.: http://participant:9090) com um prazo por envio.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

type payRequest struct {
	InstructionID string `json:"instruction_id"`
	AmountCents   int64  `json:"amount_cents"`
	Debtor        string `json:"debtor"`
	Creditor      string `json:"creditor"`
	EndToEndID    string `json:"end_to_end_id"`
}

type payResponse struct {
	Status string `json:"status"`
	Code   string `json:"code"`
}

// Pay envia a instrução. Só devolve erro nil com um resultado conclusivo.
func (c *Client) Pay(ctx context.Context, req app.PaymentRequest) (app.PaymentResult, error) {
	body, err := json.Marshal(payRequest(req))
	if err != nil {
		return app.PaymentResult{}, fmt.Errorf("%w: montando a requisição: %w", app.ErrOutcomeUnknown, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/payments", bytes.NewReader(body))
	if err != nil {
		return app.PaymentResult{}, fmt.Errorf("%w: %w", app.ErrOutcomeUnknown, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return app.PaymentResult{}, fmt.Errorf("%w: %w", app.ErrOutcomeUnknown, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 429 é a única recusa que prova "não processado": reenviar é seguro. Qualquer outro
	// não-200 continua sendo resultado desconhecido.
	if resp.StatusCode == http.StatusTooManyRequests {
		return app.PaymentResult{}, &app.ThrottledError{RetryAfter: retryAfter(resp)}
	}
	return decode(resp)
}

// retryAfter lê o Retry-After em segundos; ausente ou em outro formato vale 0.
func retryAfter(resp *http.Response) time.Duration {
	secs, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// Status consulta GET /payments/{id}. 404 é "o participante não tem registro" (not_found), que
// não prova que nada foi processado; tudo que não for uma resposta conclusiva é ErrOutcomeUnknown.
func (c *Client) Status(ctx context.Context, id string) (app.PaymentResult, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/payments/"+url.PathEscape(id), nil)
	if err != nil {
		return app.PaymentResult{}, fmt.Errorf("%w: %w", app.ErrOutcomeUnknown, err)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return app.PaymentResult{}, fmt.Errorf("%w: %w", app.ErrOutcomeUnknown, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return app.PaymentResult{Status: app.PaymentNotFound}, nil
	}
	return decode(resp)
}

// decode lê uma resposta 200 do participante (Pay e Status usam o mesmo formato).
func decode(resp *http.Response) (app.PaymentResult, error) {
	if resp.StatusCode != http.StatusOK {
		return app.PaymentResult{}, fmt.Errorf("%w: participante respondeu %d", app.ErrOutcomeUnknown, resp.StatusCode)
	}
	var out payResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil {
		return app.PaymentResult{}, fmt.Errorf("%w: resposta ilegível: %w", app.ErrOutcomeUnknown, err)
	}
	switch app.PaymentStatus(out.Status) {
	case app.PaymentAccepted:
		return app.PaymentResult{Status: app.PaymentAccepted}, nil
	case app.PaymentRejected:
		if out.Code == "" {
			return app.PaymentResult{}, fmt.Errorf("%w: rejeição sem código", app.ErrOutcomeUnknown)
		}
		return app.PaymentResult{Status: app.PaymentRejected, Code: out.Code}, nil
	default:
		return app.PaymentResult{}, fmt.Errorf("%w: status %q desconhecido", app.ErrOutcomeUnknown, out.Status)
	}
}
