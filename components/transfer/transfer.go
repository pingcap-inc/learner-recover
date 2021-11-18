package transfer

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	prom "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

type Applicable interface {
	Apply(ctx context.Context) error
}

func newPromApi(promAddr string) (v1.API, error) {
	client, err := prom.NewClient(prom.Config{
		Address: promAddr,
	})
	if err != nil {
		return nil, err
	}
	return v1.NewAPI(client), nil
}

type AddLeaders struct {
	Version  string
	PromAddr string
	PDAddr   string
	Rule     string
}

// pd -u http://172.16.105.149:2779 config placement-rules rule-bundle set pd --in=step2.json
func (a AddLeaders) addLearners(ctx context.Context) error {
	cmd := exec.Command("tiup", fmt.Sprintf("ctl:%s", a.Version), "pd",
		"-u", a.PDAddr,
		"config", "placement-rules", "rule-bundle", "set", "pd", "--in="+a.Rule,
	)
	return cmd.Run()
}

func waitCondition(ctx context.Context, api v1.API, promQL string, hit func(vector model.Vector) bool, miss func(vector model.Vector)) {
	for {
		value, _, err := api.Query(ctx, promQL, time.Now())
		if err != nil {
			log.Errorf("Fail to fetch metrics: %v", err)
			continue
		}

		if samples, ok := value.(model.Vector); ok && len(samples) > 0 {
			if hit(samples) {
				break
			}
			miss(samples)
		}

		time.Sleep(time.Second * 5)
	}
}

func (a AddLeaders) Apply(ctx context.Context) error {
	if err := a.addLearners(ctx); err != nil {
		log.Error("Fail to apply add-learners placement rules")
		return err
	}

	api, err := newPromApi(a.PromAddr)
	if err != nil {
		return err
	}

	promQL := "pd_regions_status{type=\"miss-peer-region-count\"}"
	waitCondition(ctx, api, promQL, func(vector model.Vector) bool {
		return vector[0].Value != 0
	}, func(vector model.Vector) {
		log.Info("Waiting for placement rules be applied")
	})

	waitCondition(ctx, api, promQL, func(vector model.Vector) bool {
		return vector[0].Value == 0
	}, func(vector model.Vector) {
		log.Info("Waiting for add-rule-peer fits")
	})

	log.Info("Add learners successfully")

	return nil
}
