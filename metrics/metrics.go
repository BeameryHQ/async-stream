package metrics

import (
	"expvar"
	"github.com/paulbellamy/ratecounter"
	"os"
	"sync"
	"time"
)

const (
	metricsMapKey    = "async.metrics."
	metricsPrefixEnv = "METRICS_PREFIX"
	metricsPerSec    = "_per_sec"
	metricsPerMin    = "_per_min"
	metricsCount     = "_count"
)

var once sync.Once
var prefixName string

func initPrefix() {
	prefixName = metricsMapKey
	prefix := os.Getenv(metricsPrefixEnv)
	if prefix != "" {
		prefixName = prefix + "." + prefixName
	}
}

type simpleMetric struct {
	perSec  *ratecounter.RateCounter
	perMin  *ratecounter.RateCounter
	counter *expvar.Int
	prefix  string
}

func newSimpleMetric(prefix string) *simpleMetric {
	once.Do(func() {
		initPrefix()
	})
	s := &simpleMetric{
		prefix:  prefix,
		perSec:  ratecounter.NewRateCounter(time.Second),
		perMin:  ratecounter.NewRateCounter(time.Minute),
		counter: expvar.NewInt(prefixName + prefix + metricsCount),
	}

	expvar.Publish(prefixName+s.prefix+metricsPerSec, s.perSec)
	expvar.Publish(prefixName+s.prefix+metricsPerMin, s.perMin)

	return s
}

func (s *simpleMetric) Incr() {
	s.perSec.Incr(1)
	s.perMin.Incr(1)
	s.counter.Add(1)
}
