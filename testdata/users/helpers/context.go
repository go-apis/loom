package helpers

import (
	"context"
)

type Key int

const (
	SkipSagaKey Key = iota
)

func GetSkipSaga(ctx context.Context) bool {
	skip, ok := ctx.Value(SkipSagaKey).(bool)
	return ok && skip
}
func SetSkipSaga(ctx context.Context) context.Context {
	return context.WithValue(ctx, SkipSagaKey, true)
}
