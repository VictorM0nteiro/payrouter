// producer.go
// Adapter Kafka da porta app.Publisher, sobre franz-go.
// Objetivo: publicar o lote do outbox com a chave igual ao aggregate_id (mesma partição,
// portanto em ordem) e só devolver sucesso quando o broker confirmou TODAS as mensagens.
// O produtor é idempotente (padrão do franz-go): retries internos não duplicam nem
// reordenam. Duplicatas só nascem da queda do relay entre publicar e marcar, e essas são
// absorvidas pelo consumidor, idempotente pela versão.

package kafka

import "time"

// deliveryTimeout é o prazo total para o broker confirmar um registro, retries incluídos.
const deliveryTimeout = 30 * time.Second