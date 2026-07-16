package projectors

import (
	"context"

	"github.com/go-apis/eventsourcing/es"
	"loomtest/users/data/aggregates"
	"loomtest/users/data/events"
)

type UserProjector struct {
	es.BaseProjector
}

func (u *UserProjector) ProjectExternalUserCreated(ctx context.Context, user *aggregates.User, evt *events.ExternalUserCreated) error {
	user.Type = evt.Name
	user.Username = evt.Username
	return nil
}

func (u *UserProjector) ProjectUserAdded(ctx context.Context, user *aggregates.User, evt *events.UserCreated) error {
	user.Type = "standard"
	user.Username = evt.Username
	return nil
}
func (u *UserProjector) ProjectUserDeleted(ctx context.Context, user *aggregates.User, evt *events.UserDeleted) error {
	return es.ErrDeleteEntity
}

func (u *UserProjector) ProjectEmailAdded(ctx context.Context, user *aggregates.User, evt *events.EmailAdded) error {
	user.Email = evt.Email
	return nil
}

func NewUserProjector() *UserProjector {
	return &UserProjector{}
}
