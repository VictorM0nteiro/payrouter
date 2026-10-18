// logging.go
// Logger estruturado (log/slog) e o correlation ID no contexto.
// Objetivo: cada linha de log de uma requisição carregar o mesmo correlation_id, sem que o
// código que loga precise lembrar de passá-lo. Quem recebe a requisição põe o id no contexto
// (WithCorrelationID); o handler daqui o acrescenta a todo registro emitido com esse contexto
// (logger.InfoContext(ctx, ...)). Só stdlib, para que qualquer camada possa usar, e o formato
// (JSON ou texto) é escolha de quem monta o processo.

package logging

type correlationKey struct{}

