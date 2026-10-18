// auth.go
// Autenticação de clientes: portas, erros e o caso de uso que troca credenciais por token.
// Objetivo: o cliente prova quem é uma vez (client_id + segredo) e recebe um token curto,
// usado nas demais chamadas. O caso de uso não sabe como o token é feito nem onde o cliente
// é guardado; isso fica nos adapters (JWT e Postgres). A resposta de falha é a mesma para
// cliente inexistente, segredo errado e cliente inativo, para não revelar quais ids existem.

package app

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"time"
)

var (
	// ErrInvalidCredentials: client_id ou segredo não conferem, ou o cliente está inativo.
	ErrInvalidCredentials = errors.New("credenciais inválidas")

	// ErrInvalidToken: token ausente, malformado, expirado ou com assinatura inválida.
	ErrInvalidToken = errors.New("token inválido")

	// ErrClientExists: já existe um cliente com esse id.
	ErrClientExists = errors.New("cliente já existe")
)

// Client é um cliente cadastrado da API. Guarda só o hash do segredo.
type Client struct {
	ID         string
	SecretHash []byte
	Active     bool
}

// ClientStore guarda os clientes.
type ClientStore interface {
	// Find devolve o cliente, ou ErrNotFound se não existir.
	Find(ctx context.Context, id string) (Client, error)
}

// TokenIssuer emite o token temporário de um cliente.
type TokenIssuer interface {
	Issue(clientID string) (token string, expiresIn time.Duration, err error)
}

// TokenVerifier valida um token e devolve o id do cliente dono dele. Qualquer problema
// vira ErrInvalidToken.
type TokenVerifier interface {
	Verify(token string) (clientID string, err error)
}

// HashSecret é o hash guardado no banco. O segredo é aleatório de alta entropia, então um
// SHA-256 simples basta; um hash lento só faria sentido para senhas escolhidas por pessoas.
func HashSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// dummyHash é comparado quando o cliente não existe, para o tempo de resposta não revelar
// se o id é conhecido.
var dummyHash = HashSecret("cliente-inexistente")

// AuthToken é o resultado de uma autenticação bem-sucedida.
type AuthToken struct {
	AccessToken string
	ExpiresIn   time.Duration
}

// AuthenticateClient é o caso de uso.
type AuthenticateClient struct {
	clients ClientStore
	issuer  TokenIssuer
}

func NewAuthenticateClient(clients ClientStore, issuer TokenIssuer) *AuthenticateClient {
	return &AuthenticateClient{clients: clients, issuer: issuer}
}

// Execute confere as credenciais e emite o token. Devolve ErrInvalidCredentials para
// qualquer falha de credencial; erro de infraestrutura é devolvido como veio.
func (uc *AuthenticateClient) Execute(ctx context.Context, clientID, secret string) (AuthToken, error) {
	if clientID == "" || secret == "" {
		return AuthToken{}, ErrInvalidCredentials
	}

	client, err := uc.clients.Find(ctx, clientID)
	switch {
	case errors.Is(err, ErrNotFound):
		client = Client{SecretHash: dummyHash} // inativo por omissão; só gasta o mesmo tempo
	case err != nil:
		return AuthToken{}, err
	}

	matches := subtle.ConstantTimeCompare(HashSecret(secret), client.SecretHash) == 1
	if !matches || !client.Active {
		return AuthToken{}, ErrInvalidCredentials
	}

	token, expiresIn, err := uc.issuer.Issue(client.ID)
	if err != nil {
		return AuthToken{}, err
	}
	return AuthToken{AccessToken: token, ExpiresIn: expiresIn}, nil
}
