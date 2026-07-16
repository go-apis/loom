package events

import (
	"github.com/google/uuid"
)

type UserAdded struct {
	UserId uuid.UUID
	Name   string
}
