// logging.go
// Logger estruturado (log/slog) e o correlation ID no contexto.
// Objetivo: cada linha de log de uma requisição carregar o mesmo correlation_id, sem que o
// código que loga precise lembrar de passá-lo. Quem recebe a requisição põe o id no contexto
// (WithCorrelationID); o handler daqui o acrescenta a todo registro emitido com esse contexto
// (logger.InfoContext(ctx, ...)). Só stdlib, para que qualquer camada possa usar, e o formato
// (JSON ou texto) é escolha de quem monta o processo.

package logging

import (
	"context"
	"io"
	"log/slog"
)

type correlationKey struct{}

// WithCorrelationID devolve um contexto que carrega o correlation ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID devolve o correlation ID do contexto, ou "" se não houver.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// New cria o logger. format "text" escreve texto legível; qualquer outro valor escreve JSON,
// que é o padrão para produção.
func New(format string, w io.Writer) *slog.Logger {
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(w, nil)
	} else {
		h = slog.NewJSONHandler(w, nil)
	}
	return slog.New(contextHandler{h})
}

// contextHandler acrescenta o correlation_id do contexto a cada registro.
type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs e WithGroup reembrulham o resultado, senão o logger derivado perderia o
// acréscimo do correlation_id.
func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
