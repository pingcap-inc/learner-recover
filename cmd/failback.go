package cmd

import (
	"github.com/iosmanthus/learner-recover/components/failback"
	"github.com/spf13/cobra"
)

var (
	failbackConfig string
	failbackCmd    = &cobra.Command{
		Use:   "failback",
		Short: "failback master cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := failback.WaitForStep1(failbackConfig)
			return err
		},
	}
)

func init() {
	rootCmd.AddCommand(fetchCmd)
	fetchCmd.Flags().StringVarP(&fetchConfig, "example", "c", "", "path of example file")
}
