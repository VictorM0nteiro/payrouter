// instruction.go
// O agregado PaymentInstruction: estado, tabela de transições, Rehydrate e comandos.
// Objetivo: concentrar a máquina de estados num só lugar. O agregado DECIDE (valida a
// transição e devolve o evento) e nunca grava: quem persiste é o adapter. O estado é
// sempre consequência dos eventos aplicados, nunca atribuído diretamente.

package domain

// Status é o estado atual da instrução, derivado dos eventos.
type Status string
