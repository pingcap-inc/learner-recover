package recover

import (
	"context"
	"fmt"
	"github.com/iosmanthus/learner-recover/common"
)

func (r *Resolver) ResolveConflicts(ctx context.Context, c *Config) error {
	type Target struct {
		DataDir string
		SSHPort int
		IDs     []common.RegionId
	}

	conflicts := make(map[string]*Target)
	for _, conflict := range r.conflicts {
		target := conflict.Host
		if _, ok := conflicts[target]; !ok {
			conflicts[target] = &Target{
				DataDir: conflict.DataDir,
				SSHPort: conflict.SSHPort,
			}
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

		cmd := common.SSHCommand{
			Port:         conflict.SSHPort,
			User:         c.User,
			Host:         target,
			ExtraSSHOpts: c.ExtraSSHOpts,
			CommandName:  c.TiKVCtl.Dest,
			Args: []string{
				"--data-dir", fmt.Sprintf("%s/db", conflict.DataDir), "tombstone", "--force", "-r", s,
			},
		}

		_, err := cmd.Run(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
