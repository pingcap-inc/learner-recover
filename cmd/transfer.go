package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/iosmanthus/learner-recover/common"
	"github.com/iosmanthus/learner-recover/components/transfer"
	"github.com/spf13/cobra"
)

var (
	topo           string
	clusterVersion string
	transferCmd    = &cobra.Command{
		Use:   "transfer",
		Short: "transfer master cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("missing subcommand")
		},
	}

	addLearnersRule string
	addLearners     = &cobra.Command{
		Use:   "add-learners",
		Short: "add learners to master cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			topo, err := common.ParseTiUPTopology(topo)
			if err != nil {
				return err
			}

			if len(topo.Monitors) == 0 {
				return errors.New("missing monitors")
			} else if len(topo.PDServers) == 0 {
				return errors.New("missing PD servers")
			}

			monitor := topo.Monitors[0]
			pd := topo.PDServers[0]
			action := transfer.AddLeaders{
				Version:  clusterVersion,
				PromAddr: fmt.Sprintf("http://%s:%v", monitor.Host, monitor.Port),
				PDAddr:   fmt.Sprintf("http://%s:%v", pd.Host, pd.ClientPort),
				Rule:     addLearnersRule,
			}

			return action.Apply(context.Background())
		},
	}
)

func init() {
	rootCmd.AddCommand(transferCmd)
	transferCmd.PersistentFlags().StringVarP(&topo, "topo", "", "", "topology of cluster")
	transferCmd.PersistentFlags().StringVarP(&clusterVersion, "version", "", "", "version of cluster")

	addLearners.Flags().StringVarP(&addLearnersRule, "rule", "", "", "learners rules for master cluster")
	transferCmd.AddCommand(addLearners)
}
