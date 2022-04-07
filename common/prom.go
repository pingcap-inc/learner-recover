package common

import (
	"context"
	"fmt"
	"time"

	prom "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

func NewPromApi(promAddr string) (v1.API, error) {
	client, err := prom.NewClient(prom.Config{
		Address: promAddr,
	})
	if err != nil {
		return nil, err
	}
	return v1.NewAPI(client), nil
}

func WaitCondition(ctx context.Context, api v1.API, promQL string, hit func(vector model.Vector) bool, miss func(vector model.Vector)) {
	i := 0
	promQL = fmt.Sprintf("sum(rate(%s[1m]))", promQL)
	for i < 4 {
		value, _, err := api.Query(ctx, promQL, time.Now())
		if err != nil {
			log.Errorf("Fail to fetch metrics: %v", err)
			continue
		}

		if samples, ok := value.(model.Vector); ok && len(samples) > 0 {
			if hit(samples) {
				i++
			}
			miss(samples)
		}

		time.Sleep(time.Second * 15)
	}
}
