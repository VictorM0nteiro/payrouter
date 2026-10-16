// doc_test.go
// Teste-sentinela do pacote domain.
// Objetivo: garantir que `go test -race ./...` e o CI exerçam o pacote desde o primeiro
// commit. Deve ser removido quando os testes da máquina de estados entrarem (sessão 2).

package domain

import "testing"

func TestPacoteCompila(t *testing.T) {}
