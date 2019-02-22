package metrics

import (
	"expvar"
	"github.com/paulbellamy/ratecounter"
	"os"
	"sync"
	"time"
)

const (
	metricsMapKey    = "metrics"
	metricsPrefixEnv = "METRICS_PREFIX"
	metricsPerSec    = "_per_sec"
	metricsPerMin    = "_per_min"
	metricsCount     = "_count"
)

var counts *expvar.Map
var once sync.Once

func initMap() {
	mapName := metricsMapKey
	prefix := os.Getenv(metricsPrefixEnv)
	if prefix != "" {
		mapName = prefix + "." + mapName
	}

	counts = expvar.NewMap(mapName)
}

type simpleMetric struct {
	perSec *ratecounter.RateCounter
	perMin *ratecounter.RateCounter
	prefix string
}

func newSimpleMetricWithMap(prefix string, m *expvar.Map) *simpleMetric {
	s := &simpleMetric{
		prefix: prefix,
		perSec: ratecounter.NewRateCounter(time.Second),
		perMin: ratecounter.NewRateCounter(time.Minute),
	}

	m.Set(s.prefix+metricsPerSec, s.perSec)
	m.Set(s.prefix+metricsPerMin, s.perMin)
	return s
}

func newSimpleMetric(prefix string) *simpleMetric {
	once.Do(func() {
		initMap()
	})
	return newSimpleMetricWithMap(prefix, counts)
}

func (s *simpleMetric) Incr() {
	s.perSec.Incr(1)
	s.perMin.Incr(1)
	counts.Add(s.prefix+metricsCount, 1)
}
