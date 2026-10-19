// e2e_test.go
// Testes ponta a ponta da API: servidor HTTP de verdade, casos de uso reais, tokens JWT reais
// e Postgres real em container.
// Objetivo: provar o fluxo completo que os fakes não cobrem: cadastrar cliente, autenticar,
// criar e ler de volta (leitura autoritativa, sem atraso), repetição idempotente, conflito de
// corpo, isolamento entre clientes e expiração do token. O container sobe só quando um destes
// testes roda, para os testes de handler continuarem rápidos e sem Docker.

package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	jwtauth "github.com/VictorM0nteiro/payrouter/internal/adapters/jwt"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/pgtest"
)

var e2e struct {
	once sync.Once
	pool *pgxpool.Pool
	stop func()
	err  error
}

// TestMain encerra o container no fim, se algum teste ponta a ponta o subiu.
func TestMain(m *testing.M) {
	code := m.Run()
	if e2e.stop != nil {
		e2e.stop()
	}
	os.Exit(code)
}

// env é o ambiente de um teste ponta a ponta: o servidor, o cadastro de clientes e um
// relógio que o teste pode avançar para expirar tokens.
type env struct {
	srv     *httptest.Server
	clients *postgres.ClientStore
	mu      sync.Mutex
	now     time.Time
}

func (e *env) clock() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

func (e *env) advance(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = e.now.Add(d)
}

// newEnv sobe o servidor de teste com o banco real, iniciando o container na primeira vez.
func newEnv(t *testing.T) *env {
	t.Helper()
	e2e.once.Do(func() {
		e2e.pool, e2e.stop, e2e.err = pgtest.Start(context.Background())
	})
	if e2e.err != nil {
		t.Fatalf("%v", e2e.err)
	}

	e := &env{now: time.Now(), clients: postgres.NewClientStore(e2e.pool)}
	signer, err := jwtauth.New([]byte("0123456789abcdef0123456789abcdef"), 15*time.Minute, e.clock)
	if err != nil {
		t.Fatal(err)
	}
	accept := app.NewAcceptInstruction(postgres.NewUnitOfWork(e2e.pool), func() domain.InstructionID {
		return domain.InstructionID(uuid.NewString())
	})
	get := app.NewGetInstruction(postgres.NewEventStore(e2e.pool))
	authn := app.NewAuthenticateClient(e.clients, signer)

	e.srv = httptest.NewServer(NewRoot(NewHandler(accept, get, authn, signer), e2e.pool, 5*time.Second))
	t.Cleanup(e.srv.Close)
	return e
}

// newClient cadastra um cliente novo e devolve o id e o segredo.
func (e *env) newClient(t *testing.T) (id, secret string) {
	t.Helper()
	id, secret = "cli-"+uuid.NewString(), "seg-"+uuid.NewString()
	if err := e.clients.Create(context.Background(), id, app.HashSecret(secret)); err != nil {
		t.Fatal(err)
	}
	return id, secret
}

// login autentica o cliente e devolve o access_token.
func (e *env) login(t *testing.T, id, secret string) string {
	t.Helper()
	body, _ := json.Marshal(tokenRequest{ClientID: id, ClientSecret: secret})
	status, _, data := call(t, "POST", e.srv.URL+"/auth/token", string(body), nil)
	if status != http.StatusOK {
		t.Fatalf("login status = %d, corpo %s", status, data)
	}
	var res tokenResponse
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatal(err)
	}
	return res.AccessToken
}

func bearer(token string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token}
}

func withKey(token, key string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token, "Idempotency-Key": key}
}

func call(t *testing.T, method, url, body string, headers map[string]string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, data
}

// uniqueBody gera um corpo com end_to_end_id único.
func uniqueBody() string {
	return `{"debtor":"d","creditor":"c","amount_cents":15000,"end_to_end_id":"e2e-` + uuid.NewString() + `"}`
}

func codeOf(data []byte) string {
	var e errorBody
	_ = json.Unmarshal(data, &e)
	return e.Error.Code
}

func TestE2EAutenticarCriarELerDeVolta(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)
	token := e.login(t, id, secret)

	status, hdr, data := call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), withKey(token, uuid.NewString()))
	if status != http.StatusAccepted {
		t.Fatalf("POST status = %d, corpo %s", status, data)
	}
	var created acceptResponse
	if err := json.Unmarshal(data, &created); err != nil || created.Status != "accepted" {
		t.Fatalf("POST corpo = %s (erro %v)", data, err)
	}

	// A leitura é autoritativa: o GET logo em seguida já enxerga a instrução.
	status, _, data = call(t, "GET", e.srv.URL+hdr.Get("Location"), "", bearer(token))
	if status != http.StatusOK {
		t.Fatalf("GET status = %d, corpo %s", status, data)
	}
	var got instructionResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != created.ID || got.Status != "accepted" || got.Version != 1 || got.AmountCents != 15000 || got.Attempts != 0 {
		t.Errorf("GET corpo = %+v", got)
	}
}

func TestE2ESegredoErradoEClienteInexistenteSao401Iguais(t *testing.T) {
	e := newEnv(t)
	id, _ := e.newClient(t)

	_, _, wrongSecret := call(t, "POST", e.srv.URL+"/auth/token",
		`{"client_id":"`+id+`","client_secret":"errado"}`, nil)
	status, _, unknownClient := call(t, "POST", e.srv.URL+"/auth/token",
		`{"client_id":"nao-existe","client_secret":"errado"}`, nil)

	if status != http.StatusUnauthorized || codeOf(wrongSecret) != "invalid_credentials" {
		t.Errorf("status %d, corpo %s", status, wrongSecret)
	}
	if string(wrongSecret) != string(unknownClient) {
		t.Errorf("respostas diferentes revelam se o cliente existe:\n%s\n%s", wrongSecret, unknownClient)
	}
}

func TestE2EClienteDesativadoNaoObtemToken(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)
	if err := e.clients.Deactivate(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	status, _, data := call(t, "POST", e.srv.URL+"/auth/token",
		`{"client_id":"`+id+`","client_secret":"`+secret+`"}`, nil)

	if status != http.StatusUnauthorized || codeOf(data) != "invalid_credentials" {
		t.Errorf("status %d, corpo %s", status, data)
	}
}

func TestE2ETokenExpiradoE401(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)
	token := e.login(t, id, secret)

	status, _, _ := call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), withKey(token, uuid.NewString()))
	if status != http.StatusAccepted {
		t.Fatalf("antes de expirar: status %d", status)
	}

	e.advance(15*time.Minute + time.Second)

	status, hdr, data := call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), withKey(token, uuid.NewString()))
	if status != http.StatusUnauthorized || codeOf(data) != "unauthorized" || hdr.Get("WWW-Authenticate") != "Bearer" {
		t.Errorf("depois de expirar: status %d, corpo %s", status, data)
	}

	// Autenticar de novo devolve um token que funciona.
	fresh := e.login(t, id, secret)
	status, _, _ = call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), withKey(fresh, uuid.NewString()))
	if status != http.StatusAccepted {
		t.Errorf("com token novo: status %d", status)
	}
}

func TestE2ERepeticaoDevolveAMesmaInstrucao(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)
	headers := withKey(e.login(t, id, secret), uuid.NewString())
	body := uniqueBody()

	_, hdr1, data1 := call(t, "POST", e.srv.URL+"/instructions", body, headers)
	status2, hdr2, data2 := call(t, "POST", e.srv.URL+"/instructions", body, headers)

	if status2 != http.StatusAccepted || string(data1) != string(data2) {
		t.Errorf("repetição: status %d, corpos %s e %s", status2, data1, data2)
	}
	if hdr1.Get("Idempotent-Replayed") != "" || hdr2.Get("Idempotent-Replayed") != "true" {
		t.Errorf("Idempotent-Replayed: primeira %q, segunda %q", hdr1.Get("Idempotent-Replayed"), hdr2.Get("Idempotent-Replayed"))
	}
	if hdr1.Get("Location") != hdr2.Get("Location") {
		t.Errorf("Location diferente: %q e %q", hdr1.Get("Location"), hdr2.Get("Location"))
	}
}

func TestE2ERepeticaoComTokenRenovadoContinuaIdempotente(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)
	key, body := uuid.NewString(), uniqueBody()

	_, hdr1, _ := call(t, "POST", e.srv.URL+"/instructions", body, withKey(e.login(t, id, secret), key))
	e.advance(20 * time.Minute)
	status, hdr2, _ := call(t, "POST", e.srv.URL+"/instructions", body, withKey(e.login(t, id, secret), key))

	// O scope é o cliente, não o token: renovar o token não abre uma chave nova.
	if status != http.StatusAccepted || hdr2.Get("Idempotent-Replayed") != "true" || hdr1.Get("Location") != hdr2.Get("Location") {
		t.Errorf("status %d, replay %q, locations %q e %q", status, hdr2.Get("Idempotent-Replayed"), hdr1.Get("Location"), hdr2.Get("Location"))
	}
}

func TestE2EMesmaChaveCorpoDiferenteE422(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)
	headers := withKey(e.login(t, id, secret), uuid.NewString())
	call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), headers)

	status, _, data := call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), headers)

	if status != http.StatusUnprocessableEntity || codeOf(data) != "idempotency_key_reuse" {
		t.Errorf("status %d, corpo %s", status, data)
	}
}

func TestE2EClientesDiferentesComAMesmaChaveCriamDuasInstrucoes(t *testing.T) {
	e := newEnv(t)
	idA, secretA := e.newClient(t)
	idB, secretB := e.newClient(t)
	key, body := uuid.NewString(), uniqueBody()

	_, hdrA, _ := call(t, "POST", e.srv.URL+"/instructions", body, withKey(e.login(t, idA, secretA), key))
	statusB, hdrB, _ := call(t, "POST", e.srv.URL+"/instructions", body, withKey(e.login(t, idB, secretB), key))

	if statusB != http.StatusAccepted || hdrB.Get("Idempotent-Replayed") != "" {
		t.Fatalf("o cliente B não deveria ver repetição: status %d, replay %q", statusB, hdrB.Get("Idempotent-Replayed"))
	}
	if hdrA.Get("Location") == hdrB.Get("Location") {
		t.Error("clientes diferentes com a mesma chave devem criar instruções diferentes")
	}
}

func TestE2EGetDeInstrucaoInexistenteE404(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)

	status, _, data := call(t, "GET", e.srv.URL+"/instructions/"+uuid.NewString(), "", bearer(e.login(t, id, secret)))

	if status != http.StatusNotFound {
		t.Errorf("status = %d, corpo %s", status, data)
	}
}

func TestE2ESemTokenNaoLeNemCria(t *testing.T) {
	e := newEnv(t)

	statusGet, _, _ := call(t, "GET", e.srv.URL+"/instructions/"+uuid.NewString(), "", nil)
	statusPost, _, _ := call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), map[string]string{"Idempotency-Key": "k"})

	if statusGet != http.StatusUnauthorized || statusPost != http.StatusUnauthorized {
		t.Errorf("GET %d, POST %d; esperado 401 nos dois", statusGet, statusPost)
	}
}

func TestE2EValorInvalidoNaoConsomeAChave(t *testing.T) {
	e := newEnv(t)
	id, secret := e.newClient(t)
	headers := withKey(e.login(t, id, secret), uuid.NewString())
	bad := `{"debtor":"d","creditor":"c","amount_cents":0,"end_to_end_id":"e2e-` + uuid.NewString() + `"}`

	status, _, data := call(t, "POST", e.srv.URL+"/instructions", bad, headers)
	if status != http.StatusUnprocessableEntity || codeOf(data) != "invalid_amount" {
		t.Fatalf("status %d, corpo %s", status, data)
	}

	// Corrigido, a mesma chave funciona.
	status, _, data = call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), headers)
	if status != http.StatusAccepted {
		t.Errorf("depois de corrigir: status %d, corpo %s", status, data)
	}
}

func TestE2ECorrelationIDDevolvidoELogado(t *testing.T) {
	buf := captureLogs(t)
	e := newEnv(t)
	id, secret := e.newClient(t)
	token := e.login(t, id, secret)

	headers := withKey(token, uuid.NewString())
	headers["X-Correlation-ID"] = "e2e-corr-1"
	status, hdr, _ := call(t, "POST", e.srv.URL+"/instructions", uniqueBody(), headers)

	if status != http.StatusAccepted || hdr.Get("X-Correlation-ID") != "e2e-corr-1" {
		t.Fatalf("status %d, correlation %q", status, hdr.Get("X-Correlation-ID"))
	}
	found := false
	for _, m := range logLines(t, buf) {
		if m["msg"] == "request" && m["correlation_id"] == "e2e-corr-1" && m["client_id"] == id && m["route"] == "POST /instructions" {
			found = true
		}
	}
	if !found {
		t.Errorf("linha de acesso com o correlation_id e o cliente não encontrada:\n%s", buf)
	}
}

func TestE2EReadyzComBancoEComPoolFechado(t *testing.T) {
	e := newEnv(t)

	status, _, data := call(t, "GET", e.srv.URL+"/readyz", "", nil)
	if status != http.StatusOK {
		t.Fatalf("com o banco no ar: status %d, corpo %s", status, data)
	}

	closed, err := pgxpool.NewWithConfig(context.Background(), e2e.pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	srv := httptest.NewServer(NewRoot(http.NotFoundHandler(), closed, time.Second))
	defer srv.Close()

	status, _, _ = call(t, "GET", srv.URL+"/readyz", "", nil)
	if status != http.StatusServiceUnavailable {
		t.Errorf("pool fechado: readyz = %d, esperado 503", status)
	}
	status, _, _ = call(t, "GET", srv.URL+"/healthz", "", nil)
	if status != http.StatusOK {
		t.Errorf("pool fechado: healthz = %d, esperado 200", status)
	}
}
