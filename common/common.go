package common

import (
	"context"
	"fmt"
	"github.com/pingcap/tiup/pkg/cluster/spec"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

func StringifyLabels(labels map[string]string) string {
	s := ""
	for k, v := range labels {
		s += k + "-" + v
	}
	return s
}

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

func Run(cmd *exec.Cmd, verbose bool) (string, error) {
	output, err := cmd.CombinedOutput()
	out := string(output)

	if err != nil {
		log.Warnf("%s: %v", out, err)
	} else if verbose {
		log.Info(out)
	}

	return out, err
}

func TiUP(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "tiup", args...)
	return Run(cmd, true)
}

type SSHCommand struct {
	Port         int
	User         string
	Host         string
	ExtraSSHOpts []string
	CommandName  string
	Silent       bool
	Args         []string
}

func (c *SSHCommand) Run(ctx context.Context) (string, error) {
	args := append([]string{"-p", fmt.Sprintf("%v", c.Port)}, append(c.ExtraSSHOpts, append([]string{
		fmt.Sprintf("%s@%s", c.User, c.Host), c.CommandName,
	}, c.Args...)...)...)
	cmd := exec.CommandContext(ctx, "ssh", args...)

	log.Infof("execute command: %s", cmd.String())

	return Run(cmd, !c.Silent)
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
