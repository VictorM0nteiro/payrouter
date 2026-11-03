// projection.go
// Adapter do Mongo para a projeção: um documento por instrução, na coleção "instructions".
// Objetivo: gravar os Patches da projeção de forma idempotente pela versão, sem tabela de
// deduplicação. A versão 1 insere o documento (o _id único decide a corrida); a versão v > 1 só
// atualiza se o documento está EXATAMENTE na versão v-1, e é esse filtro que faz o mesmo evento
// duas vezes não somar duas tentativas nem mudar nada, e um evento fora de ordem esperar. É o
// mesmo papel que UNIQUE(aggregate_id, version) faz no lado do comando. O horário gravado é o do
// evento (occurred_at), nunca o do relógio daqui, para a projeção ser reconstruível igual.

package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"go.mongodb.org/mongo-driver/v2/bson"
	driver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const collectionName = "instructions"

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

// EnsureIndexes cria o índice da consulta por status, que a listagem (sessão 23) vai usar.
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
func (s *ProjectionStore) Apply(ctx context.Context, p app.Patch) (app.ProjectionOutcome, error) {
	if p.Version == 1 {
		return s.insert(ctx, p)
	}

	set := bson.M{"version": p.Version, "updated_at": p.OccurredAt.UTC()}
	if p.Status != "" {
		set["status"] = string(p.Status)
	}
	if p.RejectCode != "" {
		set["reject_code"] = p.RejectCode
	}
	update := bson.M{"$set": set}
	if p.AddAttempt {
		update["$inc"] = bson.M{"attempts": 1}
	}
	res, err := s.coll.UpdateOne(ctx, bson.M{"_id": p.AggregateID, "version": p.Version - 1}, update)
	if err != nil {
		return 0, err
	}
	if res.MatchedCount == 1 {
		return app.ProjectionApplied, nil
	}

	// Não casou: ou o documento já está nesta versão (ou além), ou ainda não chegou à anterior.
	var stored struct {
		Version int `bson:"version"`
	}
	err = s.coll.FindOne(ctx, bson.M{"_id": p.AggregateID}, options.FindOne().SetProjection(bson.M{"version": 1})).Decode(&stored)
	switch {
	case errors.Is(err, driver.ErrNoDocuments):
		return app.ProjectionGap, nil
	case err != nil:
		return 0, err
	case stored.Version >= p.Version:
		return app.ProjectionDuplicate, nil
	default:
		return app.ProjectionGap, nil
	}
}

// insert cria o documento da versão 1. O _id único decide a corrida entre duas entregas.
func (s *ProjectionStore) insert(ctx context.Context, p app.Patch) (app.ProjectionOutcome, error) {
	if p.Instruction == nil {
		return 0, fmt.Errorf("patch da versão 1 de %s sem os dados da instrução", p.AggregateID)
	}
	doc := Instruction{
		ID:          p.AggregateID,
		Status:      string(p.Status),
		Version:     1,
		AmountCents: p.Instruction.AmountCents,
		Debtor:      p.Instruction.Debtor,
		Creditor:    p.Instruction.Creditor,
		EndToEndID:  p.Instruction.EndToEndID,
		UpdatedAt:   p.OccurredAt.UTC(),
	}
	if _, err := s.coll.InsertOne(ctx, doc); err != nil {
		if driver.IsDuplicateKeyError(err) {
			return app.ProjectionDuplicate, nil
		}
		return 0, err
	}
	return app.ProjectionApplied, nil
}
