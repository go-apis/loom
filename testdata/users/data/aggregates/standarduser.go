package aggregates

import (
	"context"
	"fmt"

	"github.com/go-apis/eventsourcing/es/types"
	"loomtest/users/data/commands"
	"loomtest/users/data/events"
	"loomtest/users/models"

	"github.com/go-apis/eventsourcing/es"
)

type StandardUser struct {
	es.BaseAggregateSourced `es:",snapshot=3"`

	Username    string
	Password    string
	Email       string
	Connections types.Slice[models.Connection] `gorm:"type:jsonb;serializer:json"`
	Groups      types.Slice[models.Group]      `gorm:"type:jsonb;serializer:json"`
	Deleted     bool
}

func (u *StandardUser) HandleCreate(ctx context.Context, cmd *commands.CreateUser) error {
	return u.Apply(ctx, &events.UserCreated{
		Username: cmd.Username,
		Password: cmd.Password,
	})
}
func (u *StandardUser) HandleDelete(ctx context.Context, cmd *commands.DeleteUser) error {
	return u.Apply(ctx, &events.UserDeleted{
		Deleted: true,
	})
}
func (u *StandardUser) HandleAddEmail(ctx context.Context, cmd *commands.AddEmail) error {
	return u.Apply(ctx, &events.EmailAdded{
		Email: cmd.Email,
	})
}
func (u *StandardUser) HandleAddConnection(ctx context.Context, cmd *commands.AddConnection) error {
	return u.Apply(ctx, &events.ConnectionAdded{
		Connections: types.SliceItem[models.Connection]{
			Index: len(u.Connections),
			Value: models.Connection{
				Name:     cmd.Name,
				UserId:   cmd.UserId,
				Username: cmd.Username,
			},
		},
	})
}
func (u *StandardUser) HandleAddGroup(ctx context.Context, cmd *commands.AddGroup) error {
	return u.Apply(ctx, &events.GroupAdded{
		Groups: types.SliceItem[models.Group]{
			Index: len(u.Groups),
			Value: models.Group{
				Id: cmd.GroupId,
			},
		},
	})
}
func (u *StandardUser) HandleUpdateConnection(ctx context.Context, cmd *commands.UpdateConnection) error {
	if len(u.Connections) == 0 {
		return fmt.Errorf("can't update connection")
	}

	return u.Apply(ctx, &events.ConnectionUpdated{
		Connections: types.SliceItem[models.ConnectionUpdate]{
			Index: 0,
			Value: models.ConnectionUpdate{
				Username: cmd.Username,
			},
		},
	})
}

func (u *StandardUser) ApplyEmailAdded(ctx context.Context, event *es.Event, data *events.EmailAdded) error {
	u.Email = data.Email
	return nil
}
