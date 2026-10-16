// doc.go
// Documentação do pacote domain e âncora inicial dele.
// Objetivo: reservar o pacote do núcleo do sistema (PaymentInstruction, eventos, máquina
// de estados, Money) e fazer o CI rodar algo desde o primeiro commit. O conteúdo real
// entra na sessão 2 e é escrito à mão.

// Package domain contém o agregado PaymentInstruction e suas regras, sem nenhum import de
// infraestrutura (pgx, net/http, kafka, mongo, redis). O lint (depguard) garante isso.
package domain
