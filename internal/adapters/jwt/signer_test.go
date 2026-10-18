// signer_test.go
// Testes do emissor/verificador JWT, sem banco e sem Docker.
// Objetivo: provar que o token emitido é aceito e que todo token fora do padrão é recusado:
// expirado (relógio injetado), assinatura adulterada, segredo diferente, algoritmo trocado
// ("none" e HS512), sem exp, emissor errado e sem sub.

package jwtauth

import (
	"errors"
	"strings"
	"testing"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

var (
	testSecret = []byte("0123456789abcdef0123456789abcdef")
	t0         = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
)

// clock é um relógio manual: avança só quando o teste manda.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newSigner(t *testing.T) (*Signer, *clock) {
	t.Helper()
	c := &clock{now: t0}
	s, err := New(testSecret, 15*time.Minute, c.Now)
	if err != nil {
		t.Fatal(err)
	}
	return s, c
}

// forge assina claims arbitrários com o método e a chave dados.
func forge(t *testing.T, method jwtlib.SigningMethod, key any, claims jwtlib.RegisteredClaims) string {
	t.Helper()
	tok, err := jwtlib.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func validClaims() jwtlib.RegisteredClaims {
	return jwtlib.RegisteredClaims{
		Issuer:    issuer,
		Subject:   "loja-a",
		IssuedAt:  jwtlib.NewNumericDate(t0),
		ExpiresAt: jwtlib.NewNumericDate(t0.Add(15 * time.Minute)),
	}
}

func TestEmiteEVerifica(t *testing.T) {
	s, _ := newSigner(t)

	token, expiresIn, err := s.Issue("loja-a")
	if err != nil {
		t.Fatal(err)
	}
	if expiresIn != 15*time.Minute {
		t.Errorf("expiresIn = %v", expiresIn)
	}

	id, err := s.Verify(token)
	if err != nil || id != "loja-a" {
		t.Errorf("Verify = %q, %v", id, err)
	}
}

func TestExpiracao(t *testing.T) {
	s, c := newSigner(t)
	token, _, _ := s.Issue("loja-a")

	c.now = t0.Add(14*time.Minute + 59*time.Second)
	if _, err := s.Verify(token); err != nil {
		t.Errorf("1s antes de expirar deveria valer: %v", err)
	}

	c.now = t0.Add(15*time.Minute + time.Second)
	if _, err := s.Verify(token); !errors.Is(err, app.ErrInvalidToken) {
		t.Errorf("1s depois de expirar: erro = %v, esperado ErrInvalidToken", err)
	}
}

func TestTokensInvalidos(t *testing.T) {
	s, _ := newSigner(t)
	valid, _, _ := s.Issue("loja-a")

	noExp := validClaims()
	noExp.ExpiresAt = nil
	wrongIss := validClaims()
	wrongIss.Issuer = "outro"
	noSub := validClaims()
	noSub.Subject = ""

	otherKey := []byte("ffffffffffffffffffffffffffffffff")
	tampered := valid[:len(valid)-2] + "xx"
	if tampered == valid {
		tampered = valid[:len(valid)-2] + "yy"
	}

	tests := []struct {
		name  string
		token string
	}{
		{"vazio", ""},
		{"lixo", "isto-nao-e-um-jwt"},
		{"tres_partes_invalidas", "a.b.c"},
		{"assinatura_adulterada", tampered},
		{"segredo_diferente", forge(t, jwtlib.SigningMethodHS256, otherKey, validClaims())},
		{"hs512_recusado", forge(t, jwtlib.SigningMethodHS512, testSecret, validClaims())},
		{"alg_none", forge(t, jwtlib.SigningMethodNone, jwtlib.UnsafeAllowNoneSignatureType, validClaims())},
		{"sem_exp", forge(t, jwtlib.SigningMethodHS256, testSecret, noExp)},
		{"emissor_errado", forge(t, jwtlib.SigningMethodHS256, testSecret, wrongIss)},
		{"sem_sub", forge(t, jwtlib.SigningMethodHS256, testSecret, noSub)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, err := s.Verify(tc.token)
			if !errors.Is(err, app.ErrInvalidToken) || id != "" {
				t.Errorf("Verify = %q, %v; esperado ErrInvalidToken", id, err)
			}
		})
	}
}

func TestNewRecusaConfiguracaoFraca(t *testing.T) {
	if _, err := New([]byte("curto"), time.Minute, nil); err == nil || !strings.Contains(err.Error(), "mínimo") {
		t.Errorf("segredo curto: erro = %v", err)
	}
	if _, err := New(testSecret, 0, nil); err == nil {
		t.Error("ttl zero deveria ser recusado")
	}
	if _, err := New(testSecret, time.Minute, nil); err != nil {
		t.Errorf("configuração válida com relógio nil: %v", err)
	}
}
