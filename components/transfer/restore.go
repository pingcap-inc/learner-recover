package transfer

import (
	"context"
	"github.com/iosmanthus/learner-recover/common"

	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

func (a Action) Restore(ctx context.Context) error {
	if err := a.apply(ctx); err != nil {
		log.Error("Fail to apply restore placement rules")
		return err
	}

	api, err := common.NewPromApi(a.PromAddr)
	if err != nil {
		return err
	}

	promQL := "pd_regions_status{type=\"extra-peer-region-count\"}"
	common.WaitCondition(ctx, api, promQL, func(vector model.Vector) bool {
		return vector[0].Value == 0
	}, func(vector model.Vector) {
		log.Info("Waiting for restore complete")
	})

	log.Info("Restore successfully")

	return nil
}
