// projection.go
// Adapter do Mongo para a projeção: um documento por instrução, na coleção "instructions".
// Objetivo: gravar os Patches da projeção de forma idempotente e tolerante a ordem pela versão, sem
// tabela de deduplicação. A versão v só se aplica a um documento que está EXATAMENTE na versão v-1
// (o filtro faz parte do update, sem ler-e-depois-escrever): o mesmo evento duas vezes não soma duas
// tentativas nem muda nada, e é o mesmo papel que UNIQUE(aggregate_id, version) faz no lado do
// comando. Um evento adiantado não é repetido nem descartado: fica em pending.<versão> no próprio
// documento e o evento que o precede o aplica em seguida, em cadeia (drain). Não sobra pending órfão
// porque a escrita que guarda só vale se o documento ainda está atrás de v-1 (condição no próprio
// update): ou o anterior ainda não foi aplicado, e então o documento que ele gravar já carrega o
// pending e ele o drena, ou a escrita falha e o evento é reavaliado. O horário gravado é o do evento
// (occurred_at), nunca o do relógio daqui, para a projeção ser reconstruível igual.
// Um documento que só guarda pending (o v2 chegou antes do v1) tem version 0 e nenhum status.

package mongo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	driver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	collectionName = "instructions"

	// maxAttempts limita as reavaliações quando o documento muda no meio de uma chamada.
	maxAttempts = 3

	// maxAhead é quantas versões à frente do documento um evento pode estar para ser guardado. Um
	// agregado tem dezenas de eventos no pior caso (consultas e 429 em série); passar disto é registro
	// corrompido ou de outro agregado, e guardá-lo faria o pending crescer sem limite.
	maxAhead = 256
)

// errMoved: o documento mudou durante a chamada mais vezes do que o limite; o consumidor repete.
var errMoved = errors.New("o documento mudou enquanto o evento era aplicado")

// Instruction é o documento da projeção.
type Instruction struct {
	ID          string    `bson:"_id"`
	Status      string    `bson:"status"`
	Version     int       `bson:"version"`
	AmountCents int64     `bson:"amount_cents"`
	Debtor      string    `bson:"debtor"`
	Creditor    string    `bson:"creditor"`
	EndToEndID  string    `bson:"end_to_end_id"`
	Attempts    int       `bson:"attempts"`
	RejectCode  string    `bson:"reject_code,omitempty"`
	UpdatedAt   time.Time `bson:"updated_at"`

	// Pending guarda os eventos que chegaram antes do anterior, por versão, até maxAhead à frente.
	Pending map[string]PendingPatch `bson:"pending,omitempty"`
}

// PendingPatch é um evento que chegou antes do anterior e espera nele para ser aplicado.
type PendingPatch struct {
	Status     string    `bson:"status,omitempty"`
	RejectCode string    `bson:"reject_code,omitempty"`
	AddAttempt bool      `bson:"add_attempt,omitempty"`
	OccurredAt time.Time `bson:"occurred_at"`
}

// ProjectionStore implementa app.ProjectionStore sobre o Mongo.
type ProjectionStore struct {
	client *driver.Client
	coll   *driver.Collection
}

var _ app.ProjectionStore = (*ProjectionStore)(nil)

// Connect abre o cliente e confirma que o servidor responde.
func Connect(ctx context.Context, url, database string) (*ProjectionStore, error) {
	client, err := driver.Connect(options.Client().ApplyURI(url))
	if err != nil {
		return nil, fmt.Errorf("conectando ao Mongo: %w", err)
	}
	s := &ProjectionStore{client: client, coll: client.Database(database).Collection(collectionName)}
	if err := s.Ping(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("conectando ao Mongo: %w", err)
	}
	return s, nil
}

// Ping confirma que o Mongo responde.
func (s *ProjectionStore) Ping(ctx context.Context) error { return s.client.Ping(ctx, nil) }

// Close encerra o cliente.
func (s *ProjectionStore) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

// EnsureIndexes cria o índice da consulta por status, que a listagem (sessão 23) vai usar. A
// listagem deve ignorar documentos com version 0: são só a casca de um pending.
func (s *ProjectionStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.coll.Indexes().CreateOne(ctx, driver.IndexModel{Keys: bson.D{{Key: "status", Value: 1}, {Key: "updated_at", Value: -1}}})
	return err
}

// Get devolve o documento da instrução, ou found=false se ainda não existe.
func (s *ProjectionStore) Get(ctx context.Context, id string) (doc Instruction, found bool, err error) {
	err = s.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if errors.Is(err, driver.ErrNoDocuments) {
		return Instruction{}, false, nil
	}
	return doc, err == nil, err
}

// Apply aplica o Patch. Ver o comentário do arquivo.
func (s *ProjectionStore) Apply(ctx context.Context, p app.Patch) (app.ProjectionOutcome, int, error) {
	for range maxAttempts {
		out, drained, again, err := s.applyOnce(ctx, p)
		if err != nil || !again {
			return out, drained, err
		}
	}
	return 0, 0, fmt.Errorf("%s/%d: %w", p.AggregateID, p.Version, errMoved)
}

// applyOnce faz uma avaliação. again=true pede para avaliar de novo porque o documento mudou no meio.
func (s *ProjectionStore) applyOnce(ctx context.Context, p app.Patch) (out app.ProjectionOutcome, drained int, again bool, err error) {
	if p.Version == 1 {
		return s.first(ctx, p)
	}

	doc, err := s.advance(ctx, p)
	if err == nil {
		n, err := s.drain(ctx, doc)
		return app.ProjectionApplied, n, false, err
	}
	if !errors.Is(err, driver.ErrNoDocuments) {
		return 0, 0, false, err
	}

	// O documento não está na versão anterior: já passou desta, ou ainda não chegou lá.
	cur, found, err := s.Get(ctx, p.AggregateID)
	if err != nil {
		return 0, 0, false, err
	}
	switch {
	case found && cur.Version >= p.Version:
		// Repetido. Se sobrou pending esperando (um drain interrompido por erro), aproveita para destravar.
		n, err := s.drain(ctx, cur)
		return app.ProjectionDuplicate, n, false, err
	case found && cur.Version == p.Version-1:
		return 0, 0, true, nil // o anterior acabou de ser aplicado: o filtro agora casa
	case p.Version-1-cur.Version > maxAhead: // cur é zero se o documento não existe
		return 0, 0, false, fmt.Errorf("%w: versão %d está %d à frente do documento de %s (limite %d)",
			app.ErrInvalidEvent, p.Version, p.Version-1-cur.Version, p.AggregateID, maxAhead)
	}
	return s.buffer(ctx, p)
}

// first aplica a versão 1: cria o documento, ou preenche a casca que só guardava pending.
func (s *ProjectionStore) first(ctx context.Context, p app.Patch) (app.ProjectionOutcome, int, bool, error) {
	if p.Instruction == nil {
		return 0, 0, false, fmt.Errorf("patch da versão 1 de %s sem os dados da instrução", p.AggregateID)
	}
	set := bson.M{
		"status":        string(p.Status),
		"version":       1,
		"amount_cents":  p.Instruction.AmountCents,
		"debtor":        p.Instruction.Debtor,
		"creditor":      p.Instruction.Creditor,
		"end_to_end_id": p.Instruction.EndToEndID,
		"attempts":      0,
		"updated_at":    p.OccurredAt.UTC(),
	}
	var doc Instruction
	err := s.coll.FindOneAndUpdate(ctx, bson.M{"_id": p.AggregateID, "version": 0}, bson.M{"$set": set},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	if driver.IsDuplicateKeyError(err) {
		return app.ProjectionDuplicate, 0, false, nil // o documento já está na versão 1 ou além
	}
	if err != nil {
		return 0, 0, false, err
	}
	n, err := s.drain(ctx, doc)
	return app.ProjectionApplied, n, false, err
}

// advance aplica o Patch ao documento que está exatamente na versão anterior e devolve o resultado.
// driver.ErrNoDocuments quer dizer que o documento não estava na versão anterior.
func (s *ProjectionStore) advance(ctx context.Context, p app.Patch) (Instruction, error) {
	set := bson.M{"version": p.Version, "updated_at": p.OccurredAt.UTC()}
	if p.Status != "" {
		set["status"] = string(p.Status)
	}
	if p.RejectCode != "" {
		set["reject_code"] = p.RejectCode
	}
	update := bson.M{"$set": set, "$unset": bson.M{"pending." + strconv.Itoa(p.Version): ""}}
	if p.AddAttempt {
		update["$inc"] = bson.M{"attempts": 1}
	}
	var doc Instruction
	err := s.coll.FindOneAndUpdate(ctx, bson.M{"_id": p.AggregateID, "version": p.Version - 1}, update,
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&doc)
	return doc, err
}

// buffer guarda o evento adiantado em pending.<versão>, criando a casca (version 0) se preciso. A
// condição version < v-1 é avaliada junto com a escrita: se o anterior foi aplicado no meio ela não
// casa, o upsert esbarra no _id e o evento é reavaliado; se casa, quem aplicar o anterior leva este
// pending no documento que gravar.
func (s *ProjectionStore) buffer(ctx context.Context, p app.Patch) (app.ProjectionOutcome, int, bool, error) {
	pp := PendingPatch{Status: string(p.Status), RejectCode: p.RejectCode, AddAttempt: p.AddAttempt, OccurredAt: p.OccurredAt.UTC()}
	_, err := s.coll.UpdateOne(ctx,
		bson.M{"_id": p.AggregateID, "version": bson.M{"$lt": p.Version - 1}},
		bson.M{"$set": bson.M{"pending." + strconv.Itoa(p.Version): pp}, "$setOnInsert": bson.M{"version": 0}},
		options.UpdateOne().SetUpsert(true))
	if driver.IsDuplicateKeyError(err) {
		return 0, 0, true, nil // o documento passou de v-2 enquanto isso: reavalia
	}
	if err != nil {
		return 0, 0, false, err
	}
	return app.ProjectionBuffered, 0, false, nil
}

// drain aplica, em cadeia, os eventos guardados que agora são os próximos do documento. Devolve quantos
// aplicou. Dois drains simultâneos não se atrapalham: o filtro por versão deixa um só vencer cada passo.
func (s *ProjectionStore) drain(ctx context.Context, doc Instruction) (int, error) {
	n := 0
	for {
		next := strconv.Itoa(doc.Version + 1)
		pp, ok := doc.Pending[next]
		if !ok {
			return n, nil
		}
		v := doc.Version + 1
		applied, err := s.advance(ctx, app.Patch{
			AggregateID: doc.ID, Version: v, OccurredAt: pp.OccurredAt,
			Status: domain.Status(pp.Status), RejectCode: pp.RejectCode, AddAttempt: pp.AddAttempt,
		})
		switch {
		case err == nil:
			n++
			doc = applied
		case errors.Is(err, driver.ErrNoDocuments):
			// Outro drain aplicou este passo antes: relê e segue de onde o documento está.
			cur, found, gerr := s.Get(ctx, doc.ID)
			if gerr != nil || !found || cur.Version <= doc.Version {
				return n, gerr
			}
			doc = cur
		default:
			return n, err
		}
	}
}
