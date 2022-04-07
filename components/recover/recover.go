package recover

import (
	"context"
	"errors"
	"fmt"
	"github.com/prometheus/common/model"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/iosmanthus/learner-recover/common"

	"github.com/pingcap/tiup/pkg/cluster/spec"
	log "github.com/sirupsen/logrus"
	"gopkg.in/resty.v1"
)

var ErrRetryIsExhausted = errors.New("retry is exhausted")

type status int

const (
	statusNop status = iota
	statusNeedCleanCluster
	statusPatch
	statusApplyPlacementRule
)

type Recover interface {
	UnsafeRecover

	Retry(ctx context.Context) error
	Execute(ctx context.Context) error
	Prepare(ctx context.Context) error
	Stop(ctx context.Context) error
	RebuildPD(ctx context.Context) error
	JoinTiKV(ctx context.Context) error
	PatchCluster(ctx context.Context) error
	ApplyNewPlacementRule(ctx context.Context) error
	CleanZones(ctx context.Context) error
	WaitRulesFit(ctx context.Context) error
}

type UnsafeRecover interface {
	UnsafeRecover(ctx context.Context) error
}

type ClusterRescuer struct {
	config         *Config
	currentZoneIdx int
	status
}

func NewClusterRescuer(config *Config) Recover {
	return &ClusterRescuer{config: config, status: statusNop}
}

func (r *ClusterRescuer) Prepare(ctx context.Context) error {
	config := r.config
	nodes := config.Zones[r.currentZoneIdx]

	ch := make(chan error, len(nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(nodes))
	defer wg.Wait()

	for _, node := range nodes {
		go func(node *spec.TiKVSpec) {
			defer wg.Done()

			path := fmt.Sprintf("%s@%s:%s", config.User, node.Host, config.TiKVCtl.Dest)

			log.Infof("Sending tikv-ctl to %s", node.Host)
			cmd := common.SCP{
				Port:         node.SSHPort,
				User:         config.User,
				ExtraSSHOpts: config.ExtraSSHOpts,
				Src:          []string{config.TiKVCtl.Src},
				Dest:         path,
			}

			err := cmd.Run(ctx)
			ch <- err
		}(node)
	}

	for _, node := range nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to send tikv-ctl to %s", node.Host)
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) Stop(ctx context.Context) error {
	c := r.config
	nodes := c.Zones[r.currentZoneIdx]

	ch := make(chan error, len(nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(nodes))
	defer wg.Wait()

	for _, node := range nodes {
		go func(node *spec.TiKVSpec) {
			defer wg.Done()

			log.Infof("Stoping TiKV server on %s:%v", node.Host, node.Port)
			cmd := common.SSHCommand{
				Port:         node.SSHPort,
				User:         c.User,
				Host:         node.Host,
				ExtraSSHOpts: c.ExtraSSHOpts,
				CommandName:  "sudo",
				Args: []string{
					"systemctl", "disable", "--now", fmt.Sprintf("tikv-%v.service", node.Port),
				},
			}
			_, err := cmd.Run(ctx)
			ch <- err
		}(node)
	}

	for _, node := range nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to stop TiKV server on %s:%v: %v", node.Host, node.Port, err)
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) RebuildPD(ctx context.Context) error {
	c := r.config

	log.Info("Rebuilding PD server")

	_, _ = common.TiUP(ctx, "cluster", "deploy", "-y", c.ClusterName, c.ClusterVersion, c.NewTopology.Path)

	_, err := common.TiUP(ctx, "cluster", "start", "-y", c.ClusterName)
	if err != nil {
		return err
	}

	// PDRecover
	pdServer := c.NewTopology.PDServers[0]
	cmd := exec.CommandContext(ctx, c.PDRecoverPath,
		"-endpoints", fmt.Sprintf("http://%s:%v", pdServer.Host, pdServer.ClientPort),
		"-cluster-id", c.RecoverInfoFile.ClusterID, "-alloc-id", fmt.Sprintf("%v", c.RecoverInfoFile.AllocID))
	_, err = common.Run(cmd, true)

	if err != nil {
		return err
	}

	_, err = common.TiUP(ctx, "cluster", "restart", "-y", c.ClusterName)

	if err != nil {
		return err
	}

	log.Info("Waiting PD server online")
	waitForPDServerOnline(ctx, pdServer.Host, pdServer.ClientPort)

	log.Info("Bootstrapping PD server")
	return r.pdBootstrap(ctx)
}

func waitForPDServerOnline(ctx context.Context, host string, port int) {
	client := resty.New()
	for {
		resp, err := client.R().SetContext(ctx).Get(fmt.Sprintf("http://%s:%v/pd/api/v1/config/replicate", host, port))
		if err == nil && resp.StatusCode() == http.StatusOK {
			break
		}
		time.Sleep(time.Second * 1)
	}
}

func (r *ClusterRescuer) pdBootstrap(ctx context.Context) error {
	c := r.config
	pdServer := c.NewTopology.PDServers[0]

	name := "tiup"
	args := []string{
		fmt.Sprintf("ctl:%s", c.ClusterVersion),
		"pd",
		"-u",
		fmt.Sprintf("http://%s:%v", pdServer.Host, pdServer.ClientPort),
	}
	for _, config := range c.PDBootstrap {
		extra := strings.Split(config, " ")
		cmd := exec.CommandContext(ctx, name, append(args, extra...)...)
		_, err := common.Run(cmd, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterRescuer) PatchCluster(ctx context.Context) error {
	c := r.config
	_, err := common.TiUP(ctx, "cluster", "stop", "-y", c.ClusterName)
	if err != nil {
		return err
	}

	_, err = common.TiUP(ctx, "cluster", "patch", "-y",
		c.ClusterName,
		c.Patch,
		"--offline", "--overwrite", "-R", "tikv")

	if err != nil {
		log.Errorf("fail to patch TiKV cluster: %v", err)
	}

	_, err = common.TiUP(ctx, "cluster", "start", "-y", c.ClusterName)
	return err
}

func (r *ClusterRescuer) ApplyNewPlacementRule(ctx context.Context) error {
	c := r.config
	pdServer := c.NewTopology.PDServers[0]

	name := "tiup"
	args := []string{
		fmt.Sprintf("ctl:%s", c.ClusterVersion),
		"pd",
		"-u",
		fmt.Sprintf("http://%s:%v", pdServer.Host, pdServer.ClientPort),
	}
	cmd := exec.CommandContext(ctx, name, append(args, []string{
		"config",
		"placement-rules",
		"enable",
	}...)...)

	log.Debugf("executing %s", cmd.String())
	err := cmd.Run()
	if err != nil {
		return err
	}

	cmd = exec.CommandContext(ctx, name, append(args, []string{
		"config",
		"placement-rules",
		"rule-bundle",
		"set",
		"pd",
		fmt.Sprintf("--in=%s", c.NewPlacementRules),
	}...)...)

	log.Debugf("executing %s", cmd.String())

	return cmd.Run()
}

func (r *ClusterRescuer) JoinTiKV(ctx context.Context) error {
	log.Info("Joining the TiKV servers")
	c := r.config
	_, err := common.TiUP(ctx, "cluster", "scale-out", "-y", c.ClusterName, c.JoinTopology[r.currentZoneIdx])
	return err
}

func (r *ClusterRescuer) CleanZones(ctx context.Context) error {
	c := r.config
	current := r.currentZoneIdx
	defer func() {
		r.currentZoneIdx = current
	}()
	for i := range c.Zones {
		if i != current {
			r.currentZoneIdx = i
			err := r.cleanZone(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ClusterRescuer) cleanZone(ctx context.Context) error {
	c := r.config
	err := r.Stop(ctx)
	if err != nil {
		return err
	}
	for _, node := range c.Zones[r.currentZoneIdx] {
		cmd := &common.SSHCommand{
			Port:         node.SSHPort,
			User:         c.User,
			Host:         node.Host,
			ExtraSSHOpts: c.ExtraSSHOpts,
			CommandName:  "rm",
			Silent:       false,
			Args: []string{
				"-rf", node.DataDir, node.DeployDir,
			},
		}
		_, err := cmd.Run(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterRescuer) Retry(ctx context.Context) error {
	c := r.config
	r.currentZoneIdx++
	if r.currentZoneIdx == len(c.Zones) {
		return ErrRetryIsExhausted
	}
	switch r.status {
	case statusNop:
	case statusPatch:
		log.Error("Fail to patch the cluster, please manually patch the cluster!")
	case statusApplyPlacementRule:
		log.Errorf("Fail to apply initial placement rules, please check the rules: %v", c.NewPlacementRules)
	case statusNeedCleanCluster:
		err := r.cleanCluster(ctx)
		if err != nil {
			log.Error("fail to clean last recover environment")
			return err
		}
	}
	log.Warnf("Retrying to recover another zone: %v", common.StringifyLabels(c.Labels[r.currentZoneIdx]))
	return nil
}

func (r *ClusterRescuer) cleanCluster(ctx context.Context) error {
	c := r.config
	_, err := common.TiUP(ctx, "cluster", "destroy", "-y", "--retain-role-data", "tikv", c.ClusterName)
	return err
}

func (r *ClusterRescuer) WaitRulesFit(ctx context.Context) error {
	config := r.config
	if !config.WaitRulesFit {
		return nil
	}

	monitor := config.NewTopology.Monitors[0]
	addr := fmt.Sprintf("http://%s:%v", monitor.Host, monitor.Port)
	api, err := common.NewPromApi(addr)
	if err != nil {
		return err
	}

	promQL := "pd_regions_status{type='miss-peer-region-count'}"
	common.WaitCondition(ctx, api, promQL, func(vector model.Vector) bool {
		return vector[0].Value == 0
	}, func(vector model.Vector) {
		log.Info("Waiting replicas fit")
	})
	return nil
}

func (r *ClusterRescuer) Execute(ctx context.Context) error {
	c := r.config
	log.Warnf("Recovering zone: %s", common.StringifyLabels(c.Labels[r.currentZoneIdx]))

	r.status = statusNop
	err := r.Prepare(ctx)
	if err != nil {
		log.Error("Fail to prepare tikv-ctl for TiKV learner nodes")
		return err
	}

	err = r.Stop(ctx)
	if err != nil {
		log.Error("Fail to stop the TiKV learner nodes")
		return err
	}

	err = r.UnsafeRecover(ctx)
	if err != nil {
		log.Error("Fail to recover the TiKV servers")
		return err
	}

	r.status = statusNeedCleanCluster
	err = r.RebuildPD(ctx)
	if err != nil {
		log.Error("Fail to rebuild PD")
		return err
	}

	if err := r.JoinTiKV(ctx); err != nil {
		return err
	}

	if c.Patch != "" {
		r.status = statusPatch
		log.Info("Patching TiKV cluster")
		if err := r.PatchCluster(ctx); err != nil {
			return err
		}
	}

	if c.NewPlacementRules != "" {
		r.status = statusApplyPlacementRule
		log.Info("Apply placement rules")
		if err := r.ApplyNewPlacementRule(ctx); err != nil {
			return err
		}
	}

	log.Info("Cleaning failed zones")
	err = r.CleanZones(ctx)
	if err != nil {
		log.Error("fail to clean failed zones! Please clean the zones manually.")
	}

	log.Info("Waiting placement rules fits")
	return r.WaitRulesFit(ctx)
}
