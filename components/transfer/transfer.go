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

type Action struct {
	Version  string
	PromAddr string
	PDAddr   string
	Rule     string
}

func (a Action) apply(ctx context.Context) error {
	cmd := exec.Command("tiup", fmt.Sprintf("ctl:%s", a.Version), "pd",
		"-u", a.PDAddr,
		"config", "placement-rules", "rule-bundle", "set", "pd", "--in="+a.Rule,
	)
	return cmd.Run()
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
