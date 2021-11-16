package common

import (
	"context"
	"fmt"
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

type SSHCommand struct {
	Port         int
	User         string
	Host         string
	ExtraSSHOpts []string
	CommandName  string
	Args         []string
}

func (c *SSHCommand) Run(ctx context.Context) ([]byte, error) {
	args := append([]string{"-p", fmt.Sprintf("%v", c.Port)}, append(c.ExtraSSHOpts, append([]string{
		fmt.Sprintf("%s@%s", c.User, c.Host), c.CommandName,
	}, c.Args...)...)...)
	cmd := exec.CommandContext(ctx, "ssh", args...)

	log.Infof("execute command: %s", cmd.String())

	return cmd.Output()
}

type SCP struct {
	Port         int
	User         string
	ExtraSSHOpts []string
	Src          []string
	Dest         string
}

func (s *SCP) Run(ctx context.Context) error {
	args := append([]string{"-P", fmt.Sprintf("%v", s.Port)}, s.ExtraSSHOpts...)
	cmd := exec.CommandContext(ctx, "scp", append(append(args, s.Src...), s.Dest)...)
	log.Infof("execute command: %s", cmd.String())
	return cmd.Run()
}
