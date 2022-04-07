package transfer

import (
	"context"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

func (a Action) AddLearners(ctx context.Context) error {
	if err := a.apply(ctx); err != nil {
		log.Error("Fail to apply add-learners placement rules")
		return err
	}

	api, err := newPromApi(a.PromAddr)
	if err != nil {
		return err
	}

	promQL := "pd_regions_status{type=\"miss-peer-region-count\"}"
	waitCondition(ctx, api, promQL, func(vector model.Vector) bool {
		return vector[0].Value == 0
	}, func(vector model.Vector) {
		log.Info("Waiting for add-rule-peer fits")
	})

	log.Info("Add learners successfully")

	return nil
}
