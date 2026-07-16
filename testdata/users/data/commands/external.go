package commands

import "github.com/go-apis/eventsourcing/es"

type CreateExternalUser struct {
	es.BaseCommand

	Name     string
	UserId   string
	Username string
}
