package common

import (
	"github.com/pingcap/tiup/pkg/cluster/spec"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

func IsLabelsMatch(labels map[string]string, match map[string]string) bool {
	for k, v := range labels {
		if get, ok := match[k]; !ok || get != v {
			return false
		}
	}
	return true
}

func ParseTiUPTopology(path string) (*spec.Specification, error) {
	topo := &spec.Specification{}
	if err := spec.ParseTopologyYaml(path, topo); err != nil {
		return nil, err
	}
	spec.ExpandRelativeDir(topo)

	return topo, nil
}

func Run(cmd *exec.Cmd) (string, error) {
	output, err := cmd.CombinedOutput()
	out := string(output)

	if err != nil {
		log.Warnf("%s: %v", out, err)
	} else {
		log.Info(out)
	}

	return out, err
}
