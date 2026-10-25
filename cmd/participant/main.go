// main.go
// Ponto de entrada do processo participant: lê a configuração do ambiente (ADDR,
// PARTICIPANT_BEHAVIOR, SLOW_DELAY, LATE_DELAY, REJECT_CODE, LOG_FORMAT) e sobe o participante
// simulado.
// Objetivo: dar ao sistema uma contraparte externa configurável, que pode aceitar, rejeitar,
// demorar, perder a mensagem, responder depois do timeout ou cair. O valor inválido de qualquer
// variável impede a partida, em vez de cair num padrão em silêncio.

package main

