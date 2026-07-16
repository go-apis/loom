package data

import (
	"context"

	"github.com/go-apis/eventsourcing/es"
	"loomtest/users/data/aggregates"
	"loomtest/users/data/eventhandlers"
	"loomtest/users/data/events"
	"loomtest/users/data/projectors"
	"loomtest/users/data/sagas"
)

func NewClient(ctx context.Context, pcfg *es.ProviderConfig) (es.Client, error) {
	reg, err := es.NewRegistry(
		pcfg.Service,
		&aggregates.StandardUser{},
		&aggregates.User{},
		&aggregates.ExternalUser{},
		sagas.NewConnectionSaga(),
		projectors.NewUserProjector(),
		eventhandlers.NewDemoHandler(),
		&events.GroupAdded{},
	)
	if err != nil {
		return nil, err
	}

	cli, err := es.NewClient(ctx, pcfg, reg)
	if err != nil {
		return nil, err
	}
	return cli, nil
}
