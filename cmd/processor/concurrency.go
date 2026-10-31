// concurrency.go
// Adaptador entre o limitador adaptativo (elastic.Limiter) e a porta app.Concurrency.
// Objetivo: o caso de uso (app) não importa o pool elástico; esta é a composition root que liga
// os dois e traduz o resultado de um envio (app.SendOutcome) para a regra AIMD (elastic.Outcome).

package main

import (
	"context"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
)

type concurrency struct{ limiter *elastic.Limiter }

var _ app.Concurrency = concurrency{}

func (c concurrency) Acquire(ctx context.Context) (func(app.SendOutcome), error) {
	release, err := c.limiter.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return func(o app.SendOutcome) { release(outcomeOf(o)) }, nil
}

func outcomeOf(o app.SendOutcome) elastic.Outcome {
	switch o {
	case app.SendOK:
		return elastic.Success
	case app.SendThrottled:
		return elastic.Throttled
	case app.SendUnknown:
		return elastic.Timeout
	default:
		return elastic.Unused
	}
}
