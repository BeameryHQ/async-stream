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
	rate       *ratecounter.RateCounter
	counter    *expvar.Int
	perSecRate *expvar.Int
	prefix     string
}

func newSimpleMetric(prefix string) *simpleMetric {
	once.Do(func() {
		initPrefix()
	})
	s := &simpleMetric{
		prefix:     prefix,
		rate:       ratecounter.NewRateCounter(time.Minute * 1),
		counter:    expvar.NewInt(prefixName + prefix + metricsCount),
		perSecRate: expvar.NewInt(prefixName + prefix + metricsPerSec),
	}

	expvar.Publish(prefixName+s.prefix+metricsPerMin, s.rate)

	go func() {
		ticker := time.NewTicker(time.Second * 5).C
		for range ticker {
			s.perSecRate.Set(s.rate.Rate() / 60)
		}
	}()

	return s
}

func (s *simpleMetric) Incr() {
	s.rate.Incr(1)
	s.counter.Add(1)
}
