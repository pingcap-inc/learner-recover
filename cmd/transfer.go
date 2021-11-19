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
			pd, prom, err := extractController(topo)
			if err != nil {
				return err
			}
			action := transfer.Action{
				Version:  clusterVersion,
				PDAddr:   pd,
				PromAddr: prom,
				Rule:     addLearnersRule,
			}

			return action.AddLearners(context.Background())
		},
	}

	transferLeaderRule string
	transferLeaderCmd  = &cobra.Command{
		Use:   "transfer-leader",
		Short: "transfer leaders to master cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			pd, prom, err := extractController(topo)
			if err != nil {
				return err
			}
			action := transfer.Action{
				Version:  clusterVersion,
				PDAddr:   pd,
				PromAddr: prom,
				Rule:     transferLeaderRule,
			}

			return action.TransferLeader(context.Background())
		},
	}
)

func extractController(topo string) (string, string, error) {
	t, err := common.ParseTiUPTopology(topo)
	if err != nil {
		return "", "", err
	}

	if len(t.Monitors) == 0 {
		return "", "", errors.New("missing monitors")
	} else if len(t.PDServers) == 0 {
		return "", "", errors.New("missing PD servers")
	}

	return "http://" + t.PDServers[0].Host + ":" + fmt.Sprintf("%v", t.PDServers[0].ClientPort),
		"http://" + t.Monitors[0].Host + ":" + fmt.Sprintf("%v", t.Monitors[0].Port),
		nil
}

func init() {
	rootCmd.AddCommand(transferCmd)
	transferCmd.PersistentFlags().StringVarP(&topo, "topo", "", "", "topology of cluster")
	transferCmd.PersistentFlags().StringVarP(&clusterVersion, "version", "", "", "version of cluster")

	addLearners.Flags().StringVarP(&addLearnersRule, "rule", "", "", "learners rules for master cluster")
	transferLeaderCmd.Flags().StringVarP(&transferLeaderRule, "rule", "", "", "transfer-leader rules for master cluster")

	transferCmd.AddCommand(addLearners)
	transferCmd.AddCommand(transferLeaderCmd)
}
