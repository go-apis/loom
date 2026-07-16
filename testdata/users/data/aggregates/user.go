package aggregates

import (
	"github.com/go-apis/eventsourcing/es"
)

type User struct {
	es.BaseAggregate

	Type     string
	Username string
	Email    string
}
