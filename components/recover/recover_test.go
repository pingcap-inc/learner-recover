package recover

import (
	"context"
	"flag"
	"github.com/alecthomas/assert"
	"github.com/pingcap/tiup/pkg/cluster/spec"
	"strconv"
	"strings"
	"testing"
)

var promAddr string

func init() {
	flag.StringVar(&promAddr, "prom", "", "Address of Prometheus")
}

func TestMetrics(t *testing.T) {
	flag.Parse()
	if promAddr == "" {
		return
	}
	infos := strings.Split(promAddr, ":")
	if len(infos) < 2 {
		t.Fatal("Invalid Prometheus address")
	}

	prom := &spec.PrometheusSpec{}
	prom.Host = infos[0]
	port, err := strconv.Atoi(infos[1])
	assert.Nil(t, err, "Invalid Prometheus port")
	prom.Port = port

	config := &Config{
		WaitRulesFit: true,
		NewTopology: struct {
			Path      string
			PDServers []*spec.PDSpec
			Monitors  []*spec.PrometheusSpec
		}{
			Monitors: []*spec.PrometheusSpec{prom},
		},
	}
	rescuer := NewClusterRescuer(config)
	err = rescuer.WaitRulesFit(context.Background())
	assert.Nil(t, err)
}
