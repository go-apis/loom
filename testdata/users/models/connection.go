package models

import "github.com/google/uuid"

type Connection struct {
	Name     string
	UserId   string
	Username string
}

type ConnectionUpdate struct {
	Username string
}

type Group struct {
	Id uuid.UUID
}
