package svc

import "time"

func tickerInATest() {
	t := time.NewTicker(time.Second)
	for range t.C {
	}
}
