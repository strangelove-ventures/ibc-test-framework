package main

import (
	"path"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/strangelove-ventures/localinterchain/interchain"
)

const (
	FlagAPIAddressOverride = "api-address"
	FlagAPIPortOverride    = "api-port"
)

var startCmd = &cobra.Command{
	Use:     "start <config.json>",
	Aliases: []string{"s", "run"},
	Short:   "Starts up the chain of choice with the config name",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		configPath := args[0]
		parentDir := GetDirectory()

		if path.IsAbs(configPath) {
			dir, err := filepath.Abs(configPath)
			if err != nil {
				panic(err)
			}

			parentDir = dir
			configPath = filepath.Base(configPath)
		}

		apiAddrOverride, _ := cmd.Flags().GetString(FlagAPIAddressOverride)
		apiPortOverride, _ := cmd.Flags().GetUint16(FlagAPIPortOverride)

		interchain.StartChain(parentDir, configPath, &interchain.AppOverrides{
			AddressOverride: apiAddrOverride,
			PortOverride:    apiPortOverride,
		})
	},
}

func init() {
	rootCmd.AddCommand(startCmd)

	startCmd.Flags().String(FlagAPIAddressOverride, "", "Override the default API host address")
	startCmd.Flags().Uint16(FlagAPIPortOverride, 0, "Override the default API port")
}
