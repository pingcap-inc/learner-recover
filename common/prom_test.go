package common

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	label = "test-label"
)

var (
	promAddr  = flag.String("prom", "", "addr of prometheus")
	countDown = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "recover",
		Subsystem: "test",
		Name:      "countdown",
		Help:      "metrics for test",
	}, []string{"type"})
	alwaysZero = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "recover",
		Subsystem: "test",
		Name:      "always_zero",
		Help:      "metrics for test",
	}, []string{"type"})
)

func init() {
	prometheus.MustRegister(countDown)
	prometheus.MustRegister(alwaysZero)
}

type waitConditionSuite struct {
	suite.Suite
	server *http.Server
	done   chan<- struct{}
}

func TestWaitConditionSuite(t *testing.T) {
	if *promAddr == "" {
		t.Skip()
	}
	suite.Run(t, new(waitConditionSuite))
}

func (s *waitConditionSuite) SetupTest() {
	http.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr: ":2112",
	}
	done := make(chan struct{}, 1)
	s.server = server
	s.done = done
	go func() {
		_ = server.ListenAndServe()
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		i := 1
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if i != 0 && i < 4 {
					i++
				} else {
					i = 0
				}
				countDown.WithLabelValues(label).Set(float64(i))
				alwaysZero.WithLabelValues(label).Set(0)
			}
		}
	}()
}

func (s *waitConditionSuite) TestWaitConditionCountDown() {
	ctx := context.Background()
	api, err := NewPromApi(*promAddr)
	assert.Nil(s.T(), err)

	done := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	i := 0
	promQL := fmt.Sprintf("recover_test_countdown{type=\"%s\"}", label)
	go func() {
		WaitCondition(ctx, api, promQL, func(vector model.Vector) bool {
			log.Infof("i: %v", i)
			log.Infof("samples: %s", vector.String())
			if vector[0].Value == 0 {
				i++
				return true
			}
			return false
		}, func(vector model.Vector) {
			log.Info("condition missed, waiting for next check")
		})
		assert.Equal(s.T(), 4, i)
		done <- struct{}{}
	}()
	select {
	case <-timeout.Done():
		assert.Fail(s.T(), "test timeout")
	case <-done:
	}
}

func (s *waitConditionSuite) TestWaitConditionAlwaysZero() {
	ctx := context.Background()
	api, err := NewPromApi(*promAddr)
	assert.Nil(s.T(), err)

	done := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	i := 0
	promQL := fmt.Sprintf("recover_test_always_zero{type=\"%s\"}", label)
	go func() {
		WaitCondition(ctx, api, promQL, func(vector model.Vector) bool {
			log.Infof("i: %v", i)
			log.Infof("samples: %s", vector.String())
			if vector[0].Value == 0 {
				i++
				return true
			}
			return false
		}, func(vector model.Vector) {
			log.Info("condition missed, waiting for next check")
		})
		assert.Equal(s.T(), 4, i)
		done <- struct{}{}
	}()
	select {
	case <-timeout.Done():
		assert.Fail(s.T(), "test timeout")
	case <-done:
	}
}

func (s *waitConditionSuite) TearDownTest() {
	s.server.Close()
	s.done <- struct{}{}
}
