// signer.go
// Emissor e verificador de tokens JWT HS256 (app.TokenIssuer e app.TokenVerifier).
// Objetivo: o token é a prova temporária de que o cliente se autenticou. Carrega o id do
// cliente (sub) e expira sozinho (exp). A verificação trava o algoritmo em HS256, o que
// barra o ataque clássico de "alg: none" e de troca de algoritmo, e exige exp, iss e sub. O
// relógio é injetável para testar expiração sem esperar.

package jwtauth

import (
	"errors"
	"fmt"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

const (
	issuer = "payrouter"

	// MinSecretBytes é o tamanho mínimo do segredo de assinatura: 256 bits para HS256.
	MinSecretBytes = 32
)

// Signer emite e verifica tokens com o mesmo segredo.
type Signer struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

var (
	_ app.TokenIssuer   = (*Signer)(nil)
	_ app.TokenVerifier = (*Signer)(nil)
)

// New cria o Signer. now pode ser nil (usa time.Now).
func New(secret []byte, ttl time.Duration, now func() time.Time) (*Signer, error) {
	if len(secret) < MinSecretBytes {
		return nil, fmt.Errorf("segredo de assinatura com %d bytes: o mínimo é %d", len(secret), MinSecretBytes)
	}
	if ttl <= 0 {
		return nil, errors.New("a validade do token deve ser positiva")
	}
	if now == nil {
		now = time.Now
	}
	return &Signer{secret: secret, ttl: ttl, now: now}, nil
}

// Issue emite um token para o cliente, válido por ttl.
func (s *Signer) Issue(clientID string) (string, time.Duration, error) {
	now := s.now()
	claims := jwtlib.RegisteredClaims{
		Issuer:    issuer,
		Subject:   clientID,
		IssuedAt:  jwtlib.NewNumericDate(now),
		ExpiresAt: jwtlib.NewNumericDate(now.Add(s.ttl)),
	}
	signed, err := jwtlib.NewWithClaims(jwtlib.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", 0, fmt.Errorf("assinando token: %w", err)
	}
	return signed, s.ttl, nil
}

// Verify valida assinatura, algoritmo, emissor e validade, e devolve o id do cliente.
// Qualquer falha vira app.ErrInvalidToken.
func (s *Signer) Verify(token string) (string, error) {
	parser := jwtlib.NewParser(
		jwtlib.WithValidMethods([]string{jwtlib.SigningMethodHS256.Alg()}),
		jwtlib.WithIssuer(issuer),
		jwtlib.WithExpirationRequired(),
		jwtlib.WithTimeFunc(s.now),
	)

	var claims jwtlib.RegisteredClaims
	if _, err := parser.ParseWithClaims(token, &claims, func(*jwtlib.Token) (any, error) {
		return s.secret, nil
	}); err != nil {
		return "", fmt.Errorf("%w: %w", app.ErrInvalidToken, err)
	}
	if claims.Subject == "" {
		return "", fmt.Errorf("%w: sem sub", app.ErrInvalidToken)
	}
	return claims.Subject, nil
}
