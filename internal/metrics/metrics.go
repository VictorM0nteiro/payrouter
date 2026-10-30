// metrics.go
// Métricas Prometheus do processor e do relay, portadas do jeito do wallet-go (registry próprio
// por binário, recorders que não fazem nada quando nulos). É reuso declarado, não contribuição
// deste projeto.
// Objetivo: tornar visível o que só existe porque a fronteira é assíncrona: 429 contra timeout
// lado a lado, a espera do limite de saída, o atraso do outbox e a fila de conciliação.
// Os labels são sempre conjuntos fechados; NUNCA aggregate_id ou instruction_id (cada valor novo
// criaria uma série nova para sempre). O domínio não importa este pacote.

package metrics

const namespace = "payrouter"

