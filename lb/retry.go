package lb

import (
	"github.com/cenkalti/backoff"
	"time"
)

func RetryNormal(cb func() error) error {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = time.Second * 15

	return backoff.Retry(cb, b)
}
