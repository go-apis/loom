package loomgen

import "time"

func generated() {
	t := time.NewTicker(time.Second)
	for range t.C {
	}
}
