package recover

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/tiup/pkg/cluster/spec"

	"github.com/iosmanthus/learner-recover/common"

	log "github.com/sirupsen/logrus"
)

func (r *ClusterRescuer) dropLogs(ctx context.Context) error {
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

			path := fmt.Sprintf("%s/db", node.DataDir)
			log.Infof("Dropping raft logs of TiKV server on %s:%v:%s", node.Host, node.Port, path)
			cmd := common.SSHCommand{
				Port:         node.SSHPort,
				User:         config.User,
				Host:         node.Host,
				ExtraSSHOpts: config.ExtraSSHOpts,
				CommandName:  config.TiKVCtl.Dest,
				Args: []string{
					"--db", path, "unsafe-recover", "drop-unapplied-raftlog", "--all-regions",
				},
			}

			_, err := cmd.Run(ctx)
			ch <- err
		}(node)
	}

	for i := 0; i < len(config.Nodes); i++ {
		if err := <-ch; err != nil {
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) promoteLearner(ctx context.Context) error {
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

			path := fmt.Sprintf("%s/db", node.DataDir)
			cmd := common.SSHCommand{
				Port:         node.SSHPort,
				User:         config.User,
				Host:         node.Host,
				ExtraSSHOpts: config.ExtraSSHOpts,
				CommandName:  config.TiKVCtl.Dest,
				Args: []string{
					"--db", path, "store",
				},
			}
			storeIDBytes, err := cmd.Run(ctx)
			if err != nil {
				ch <- fmt.Errorf("fail to read store id of %s:%v", node.Host, node.Port)
				return
			}

			storeIDStr := string(storeIDBytes)
			storeIDStr = strings.TrimPrefix(storeIDStr, "store id: ")
			storeIDStr = strings.TrimSuffix(storeIDStr, "\n")

			self, err := strconv.ParseUint(storeIDStr, 10, 64)
			if err != nil {
				ch <- fmt.Errorf("invalid store id: %v, from %s:%v: %v", storeIDStr, node.Host, node.Port, err)
				return
			}

			// remove-fail-stores --promote-learner --all-regions
			log.Infof("Promoting learners of TiKV server on %s:%v", node.Host, node.Port)

			var (
				i      int
				stores string
			)
			for _, store := range config.RecoverInfoFile.StoreIDs {
				if store == self {
					continue
				}
				if i == 0 {
					stores += fmt.Sprintf("%v", store)
				} else {
					stores += fmt.Sprintf(",%v", store)
				}
				i++
			}

			cmd = common.SSHCommand{
				Port:         node.SSHPort,
				User:         config.User,
				Host:         node.Host,
				ExtraSSHOpts: config.ExtraSSHOpts,
				CommandName:  config.TiKVCtl.Dest,
				Args: []string{
					"--db", path, "unsafe-recover",
					"remove-fail-stores", "--promote-learner", "--all-regions", "-s", stores,
				},
			}

			_, err = cmd.Run(ctx)
			ch <- err
		}(node)
	}

	for _, node := range config.Nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to promote learners of TiKV server on %s:%v: %v", node.Host, node.Port, err)
			return err
		}
	}

	return nil
}

type RemoteTiKVCtl struct {
	Controller   string
	ExtraSSHOpts []string
	DataDir      string
	User         string
	Host         string
	SSHPort      int
}

func (c *RemoteTiKVCtl) Fetch(ctx context.Context) (*common.RegionInfos, error) {
	log.Infof("fetching region infos from: %s", c.Host)

	cmd := common.SSHCommand{
		Port:         c.SSHPort,
		User:         c.User,
		Host:         c.Host,
		ExtraSSHOpts: c.ExtraSSHOpts,
		CommandName:  c.Controller,
		Args: []string{
			"--db", fmt.Sprintf("%s/db", c.DataDir), "raft", "region", "--all-regions",
		},
	}

	resp, err := cmd.Run(ctx)
	if err != nil {
		log.Errorf("fail to fetch region infos from %s: %v", c.Host, err)
		return nil, err
	}

	infos := &common.RegionInfos{}
	if err = json.Unmarshal([]byte(resp), infos); err != nil {
		return nil, err
	}

	for id := range infos.StateMap {
		infos.StateMap[id].Host = c.Host
		infos.StateMap[id].DataDir = c.DataDir
		infos.StateMap[id].SSHPort = c.SSHPort
	}

	log.Infof("[DONE] fetching region infos from: %s", c.Host)

	return infos, nil
}

func (r *ClusterRescuer) UnsafeRecover(ctx context.Context) error {
	c := r.config

	err := r.dropLogs(ctx)
	if err != nil {
		return err
	}

	collector := common.NewRegionCollector()

	var fetchers []common.Fetcher
	for _, node := range c.Nodes {
		fetcher := &RemoteTiKVCtl{
			Controller:   c.TiKVCtl.Dest,
			ExtraSSHOpts: c.ExtraSSHOpts,
			DataDir:      node.DataDir,
			User:         c.User,
			Host:         node.Host,
			SSHPort:      node.SSHPort,
		}
		fetchers = append(fetchers, fetcher)
	}

	log.Info("fetching region infos")
	resolver := NewResolver()

	_, err = collector.Collect(ctx, fetchers, resolver)
	if err != nil {
		return err
	}

	log.Warn("resolving region conflicts")
	err = resolver.TryResolve()
	if err != nil {
		return err
	}
	err = resolver.ResolveConflicts(ctx, c)
	if err != nil {
		return err
	}

	return r.promoteLearner(ctx)
}
