package metrics

import "expvar"

const (
	consumersOnline = "consumers_online"
)

func SetOnline() {
	val := new(expvar.Int)
	val.Set(1)
	counts.Set(consumersOnline, val)
}

func SetOffline() {
	val := new(expvar.Int)
	val.Set(0)
	counts.Set(consumersOnline, val)
}
