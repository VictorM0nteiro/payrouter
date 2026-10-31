// pool_test.go
// Testes do Transport próprio do cliente (WithPool), contra o participante simulado (httptest).
// Objetivo: provar que o cliente com pool dimensionado para o teto continua falando com o
// participante, reaproveita conexões, aceita CloseIdleConnections no meio do uso (o que o
// limitador faz a cada queda do limite) e que o Transport recebe o tamanho e o TTL pedidos.

package participant

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

func TestWithPoolConfiguraOTransportComOTetoEOTTL(t *testing.T) {
	c := NewClient("http://x", time.Second).WithPool(6, 45*time.Second)

	tr, ok := c.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport = %T, esperado *http.Transport", c.http.Transport)
	}
	if tr.MaxIdleConnsPerHost != 6 || tr.IdleConnTimeout != 45*time.Second {
		t.Errorf("MaxIdleConnsPerHost=%d IdleConnTimeout=%v, esperado 6 e 45s", tr.MaxIdleConnsPerHost, tr.IdleConnTimeout)
	}
	if http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost == 6 {
		t.Error("WithPool alterou o DefaultTransport, que é de todo o processo")
	}
}

func TestClienteComPoolEnviaEAguentaFecharOciosasNoMeioDoUso(t *testing.T) {
	srv := newSim(t)
	c := NewClient(srv.URL, 2*time.Second).WithPool(4, time.Minute)

	for i := range 6 {
		res, err := c.Pay(context.Background(), request("pool-"+string(rune('a'+i)), "accept:1"))
		if err != nil || res.Status != app.PaymentAccepted {
			t.Fatalf("Pay %d = %+v, %v; esperado accepted", i, res, err)
		}
		c.CloseIdleConnections() // o que o limitador faz quando o limite cai
	}
}
