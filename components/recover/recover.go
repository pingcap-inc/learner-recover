package recover

import (
	"context"
	"fmt"
	"github.com/pingcap/tiup/pkg/cluster/spec"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/iosmanthus/learner-recover/common"
	log "github.com/sirupsen/logrus"
	"gopkg.in/resty.v1"
)

type Recover interface {
	UnsafeRecover

	Execute(ctx context.Context) error
	Prepare(ctx context.Context) error
	Stop(ctx context.Context) error
	RebuildPD(ctx context.Context) error
	Finish(ctx context.Context) error
}

type UnsafeRecover interface {
	UnsafeRecover(ctx context.Context) error
}

type ClusterRescuer struct {
	config *Config
}

func NewClusterRescuer(config *Config) Recover {
	return &ClusterRescuer{config}
}

func (r *ClusterRescuer) Prepare(ctx context.Context) error {
	config := r.config

	ch := make(chan error, len(config.Nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(config.Nodes))
	defer wg.Wait()

	for _, node := range config.Nodes {
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

	for _, node := range config.Nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to send tikv-ctl to %s", node.Host)
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) Stop(ctx context.Context) error {
	config := r.config

	ch := make(chan error, len(config.Nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(config.Nodes))
	defer wg.Wait()

	for _, node := range config.Nodes {
		go func(node *spec.TiKVSpec) {
			defer wg.Done()

			log.Infof("Stoping TiKV server on %s:%v", node.Host, node.Port)
			cmd := common.SSHCommand{
				Port:         node.SSHPort,
				User:         config.User,
				Host:         node.Host,
				ExtraSSHOpts: config.ExtraSSHOpts,
				CommandName:  "sudo",
				Args: []string{
					"systemctl", "disable", "--now", fmt.Sprintf("tikv-%v.service", node.Port),
				},
			}
			_, err := cmd.Run(ctx)
			ch <- err
		}(node)
	}

	for _, node := range config.Nodes {
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

func (r *ClusterRescuer) patchCluster(ctx context.Context) error {
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

func (r *ClusterRescuer) applyNewPlacementRule(ctx context.Context) error {
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

func (r *ClusterRescuer) joinTiKV(ctx context.Context) error {
	c := r.config
	_, err := common.TiUP(ctx, "cluster", "scale-out", "-y", c.ClusterName, c.JoinTopology)
	return err
}

func (r *ClusterRescuer) Finish(ctx context.Context) error {
	c := r.config

	log.Info("Joining the TiKV servers")
	if err := r.joinTiKV(ctx); err != nil {
		return err
	}

	// Patch the cluster before finishing
	if c.Patch != "" {
		log.Info("Patching TiKV cluster")
		if err := r.patchCluster(ctx); err != nil {
			return err
		}
	}

	if c.NewPlacementRules != "" {
		log.Info("Apply placement rules")
		if err := r.applyNewPlacementRule(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) Execute(ctx context.Context) error {
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

	err = r.RebuildPD(ctx)
	if err != nil {
		log.Error("Fail to rebuild PD")
		return err
	}

	return r.Finish(ctx)
}
