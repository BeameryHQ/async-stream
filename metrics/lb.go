package metrics

import "expvar"

const (
	consumersOnlineKey      = "consumers_online"
	consumersOnlineCountKey = "consumers_online_count"
)

var (
	consumersOnlineGauge = expvar.NewInt(prefixName + consumersOnlineKey)
	consumersOnlineCount = expvar.NewInt(prefixName + consumersOnlineCountKey)
)

func SetOnline() {
	consumersOnlineGauge.Set(1)
}

func SetOffline() {
	consumersOnlineGauge.Set(0)
}


func IncrConsumersOnline(){
	consumersOnlineCount.Add(1)
}

func DecrConsumersOnline(){
	consumersOnlineCount.Add(-1)
}
