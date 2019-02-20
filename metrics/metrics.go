package metrics

import (
	"expvar"
	"github.com/paulbellamy/ratecounter"
	"os"
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

func init() {
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

func newSimpleMetric(prefix string) *simpleMetric {
	return &simpleMetric{
		prefix: prefix,
		perSec: ratecounter.NewRateCounter(time.Second),
		perMin: ratecounter.NewRateCounter(time.Minute),
	}
}

func (s *simpleMetric) Incr() {
	s.perSec.Incr(1)
	current := s.perSec.Rate()
	counts.Add(s.prefix+metricsCount, 1)

	gaugeSec := new(expvar.Int)
	gaugeSec.Set(current)
	counts.Set(s.prefix+metricsPerSec, gaugeSec)

	s.perMin.Incr(1)
	currentMin := s.perMin.Rate()
	gaugeMin := new(expvar.Int)
	gaugeMin.Set(currentMin)
	counts.Set(s.prefix+metricsPerMin, gaugeMin)
}
