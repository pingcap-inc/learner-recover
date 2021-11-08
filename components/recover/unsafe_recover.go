package recover

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"sync"

	"github.com/pingcap/tiup/pkg/cluster/spec"

	"github.com/iosmanthus/learner-recover/common"

	log "github.com/sirupsen/logrus"
)

type ResolveConflicts struct {
	conflicts []*common.RegionState
	resolved  []*common.RegionState
}

func NewResolveConflicts() *ResolveConflicts {
	return &ResolveConflicts{}
}

func (r *ResolveConflicts) ResolveConflicts(ctx context.Context, c *Config) error {
	type Target struct {
		DataDir string
		IDs     []common.RegionId
	}

	conflicts := make(map[string]*Target)
	for _, conflict := range r.conflicts {
		target := fmt.Sprintf("%s@%s", c.User, conflict.Host)
		if _, ok := conflicts[target]; !ok {
			conflicts[target] = &Target{DataDir: conflict.DataDir}
		}
		conflicts[target].IDs = append(conflicts[target].IDs, conflict.RegionId)
	}

	for target, conflict := range conflicts {
		s := ""
		for i, id := range conflict.IDs {
			if i == 0 {
				s = fmt.Sprintf("%v", id)
			} else {
				s += fmt.Sprintf(",%v", id)
			}
		}

		cmd := exec.CommandContext(ctx,
			"ssh", "-p", fmt.Sprintf("%v", c.SSHPort), target,
			c.TiKVCtl.Dest, "--db", fmt.Sprintf("%s/db", conflict.DataDir), "tombstone", "--force", "-r", s)

		_, err := common.Run(cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ResolveConflicts) Merge(_ *common.RegionInfos, b *common.RegionInfos) *common.RegionInfos {
	toBeResolved := make([]*common.RegionState, 0)
	newResolved := make([]*common.RegionState, 0)

	for _, state := range b.StateMap {
		toBeResolved = append(toBeResolved, state)
	}

	for {
		hasConflict := false

		for _, state := range toBeResolved {
			var other *common.RegionState
			otherIdx := -1
			if len(r.resolved) != 0 {
				if state.LocalState.Region.EndKey == "" {
					other = r.resolved[len(r.resolved)-1]
					otherIdx = len(r.resolved) - 1
				} else {
					i := sort.Search(len(r.resolved), func(i int) bool { return r.resolved[i].LocalState.Region.StartKey < state.LocalState.Region.EndKey })
					if i < len(r.resolved) {
						other = r.resolved[i]
						otherIdx = i
					}
				}
			}

			if other != nil &&
				(other.LocalState.Region.EndKey == "" ||
					other.LocalState.Region.EndKey > state.LocalState.Region.StartKey) {
				hasConflict = true
				// resolve conflicts
				version1 := state.LocalState.Region.RegionEpoch.Version
				version2 := other.LocalState.Region.RegionEpoch.Version
				if version1 > version2 ||
					(version1 == version2 && state.ApplyState.AppliedIndex >= other.ApplyState.AppliedIndex) {

					// remove from resolved
					r.resolved = append(r.resolved[:otherIdx], r.resolved[otherIdx+1:]...)
					newResolved = append(newResolved, state)
					r.conflicts = append(r.conflicts, other)

				} else {
					r.conflicts = append(r.conflicts, state)
				}
			} else {
				newResolved = append(newResolved, state)
			}
		}

		if hasConflict {
			toBeResolved = newResolved
			newResolved = make([]*common.RegionState, 0)
		} else {
			r.resolved = append(r.resolved, newResolved...)
			sort.SliceStable(r.resolved, func(i, j int) bool {
				return r.resolved[i].LocalState.Region.StartKey > r.resolved[j].LocalState.Region.StartKey
			})
			break
		}
	}

	return nil
}

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
			cmd := exec.CommandContext(ctx,
				"ssh", "-p", fmt.Sprintf("%v", config.SSHPort), fmt.Sprintf("%s@%s", config.User, node.Host),
				config.TiKVCtl.Dest, "--db", path, "unsafe-recover", "drop-unapplied-raftlog", "--all-regions")
			_, err := common.Run(cmd)
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

			// remove-fail-stores --promote-learner --all-regions
			log.Infof("Promoting learners of TiKV server on %s:%v", node.Host, node.Port)

			var stores string
			for i, store := range config.RecoverInfoFile.StoreIDs {
				if i == 0 {
					stores += fmt.Sprintf("%v", store)
				} else {
					stores += fmt.Sprintf(",%v", store)
				}
			}

			path := fmt.Sprintf("%s/db", node.DataDir)
			cmd := exec.CommandContext(ctx,
				"ssh", "-p", fmt.Sprintf("%v", config.SSHPort), fmt.Sprintf("%s@%s", config.User, node.Host),
				config.TiKVCtl.Dest, "--db", path, "unsafe-recover",
				"remove-fail-stores", "--promote-learner", "--all-regions", "-s", stores)

			_, err := common.Run(cmd)
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
	Controller string
	DataDir    string
	User       string
	Host       string
	SSHPort    int
}

func (c *RemoteTiKVCtl) Fetch(ctx context.Context) (*common.RegionInfos, error) {
	log.Infof("fetching region infos from: %s", c.Host)
	cmd := exec.CommandContext(ctx,
		"ssh", "-p", fmt.Sprintf("%v", c.SSHPort), fmt.Sprintf("%s@%s", c.User, c.Host),
		c.Controller, "--db", fmt.Sprintf("%s/db", c.DataDir), "raft", "region", "--all-regions")

	resp, err := cmd.Output()
	if err != nil {
		log.Errorf("fail to fetch region infos from %s: %v", c.Host, err)
		return nil, err
	}

	infos := &common.RegionInfos{}
	if err = json.Unmarshal(resp, infos); err != nil {
		return nil, err
	}

	for id := range infos.StateMap {
		infos.StateMap[id].Host = c.Host
		infos.StateMap[id].DataDir = c.DataDir
	}

	log.Infof("[完成] fetching region infos from: %s", c.Host)

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
			Controller: c.TiKVCtl.Dest,
			DataDir:    node.DataDir,
			User:       c.User,
			Host:       node.Host,
			SSHPort:    c.SSHPort,
		}
		fetchers = append(fetchers, fetcher)
	}

	log.Info("fetching region infos")
	resolver := NewResolveConflicts()

	_, err = collector.Collect(ctx, fetchers, resolver)
	if err != nil {
		return err
	}

	log.Warn("resolving region conflicts")
	err = resolver.ResolveConflicts(ctx, c)
	if err != nil {
		return err
	}

	return r.promoteLearner(ctx)
}
