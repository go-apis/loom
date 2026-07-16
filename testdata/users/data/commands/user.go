package commands

import (
	"github.com/go-apis/eventsourcing/es"
	"github.com/google/uuid"
)

type CreateUser struct {
	es.BaseCommand
	es.BaseNamespaceCommand

	Username string
	Password string
}

type DeleteUser struct {
	es.BaseCommand
	es.BaseNamespaceCommand
}

type AddEmail struct {
	es.BaseCommand

	Email string
}

type AddConnection struct {
	es.BaseCommand

	Name     string
	UserId   string
	Username string
}

type UpdateConnection struct {
	es.BaseCommand

	Username string
}

type AddGroup struct {
	es.BaseCommand

	GroupId uuid.UUID `json:"group_id"`
}
