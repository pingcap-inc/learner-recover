package failback

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	prom "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	log "github.com/sirupsen/logrus"
	"time"
)

func WaitForStep1(promAddr string) error {
	client, err := prom.NewClient(prom.Config{
		Address: promAddr,
	})
	if err != nil {
		return err
	}

	api := v1.NewAPI(client)
	ctx := context.Background()
	value, _, err := api.Query(ctx, "pd_regions_status", time.Now())
	if err != nil {
		return err
	}
	log.Info(spew.Sdump(value))
	return nil
}
