package metrics

import "expvar"

const (
	consumersOnline = "consumers_online"
)

var (
	consumersOnlineGauge = expvar.NewInt(prefixName + consumersOnline)
)

func SetOnline() {
	consumersOnlineGauge.Set(1)
}

func SetOffline() {
	consumersOnlineGauge.Set(0)
}
