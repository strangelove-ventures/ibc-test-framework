package ibctest_test

import (
	"context"
	"fmt"
	"testing"

	transfertypes "github.com/cosmos/ibc-go/v3/modules/apps/transfer/types"
	"github.com/strangelove-ventures/ibctest"
	"github.com/strangelove-ventures/ibctest/ibc"
	"github.com/strangelove-ventures/ibctest/test"
	"github.com/strangelove-ventures/ibctest/testreporter"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestPacketForwardMiddleware(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	pool, network := ibctest.DockerSetup(t)

	rep := testreporter.NewNopReporter()
	eRep := rep.RelayerExecReporter(t)

	ctx := context.Background()

	cf := ibctest.NewBuiltinChainFactory([]ibctest.BuiltinChainFactoryEntry{
		{Name: "gaia", NameOverride: "gaia-fork", Version: "bugfix-replace_default_transfer_with_router_module", ChainID: "cosmoshub-0", NumValidators: 4, NumFullNodes: 0},
		{Name: "osmosis", NameOverride: "osmosis", Version: "v7.3.0", ChainID: "osmosis-1", NumValidators: 4, NumFullNodes: 0},
		{Name: "juno", NameOverride: "juno", Version: "v6.0.0", ChainID: "juno-1", NumValidators: 4, NumFullNodes: 0},
	}, zaptest.NewLogger(t))

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)

	gaia, osmosis, juno := chains[0], chains[1], chains[2]

	r := ibctest.NewBuiltinRelayerFactory(ibc.CosmosRly, zaptest.NewLogger(t)).Build(
		t, pool, network, home,
	)

	const pathOsmoHub = "osmohub"
	const pathJunoHub = "junohub"

	ic := ibctest.NewInterchain().
		AddChain(osmosis).
		AddChain(gaia).
		AddChain(juno).
		AddRelayer(r, "relayer").
		AddLink(ibctest.InterchainLink{
			Chain1:  osmosis,
			Chain2:  gaia,
			Relayer: r,
			Path:    pathOsmoHub,
		}).
		AddLink(ibctest.InterchainLink{
			Chain1:  gaia,
			Chain2:  juno,
			Relayer: r,
			Path:    pathJunoHub,
		})

	require.NoError(t, ic.Build(ctx, eRep, ibctest.InterchainBuildOptions{
		TestName:  t.Name(),
		HomeDir:   home,
		Pool:      pool,
		NetworkID: network,

		SkipPathCreation: false,
	}))

	const userFunds = int64(10_000_000_000)
	users := ibctest.GetAndFundTestUsers(t, ctx, t.Name(), userFunds, osmosis, gaia, juno)

	channels, err := r.GetChannels(ctx, eRep, gaia.Config().ChainID)
	require.NoError(t, err)

	err = r.StartRelayer(ctx, eRep, pathOsmoHub)
	require.NoError(t, err)

	err = r.StartRelayer(ctx, eRep, pathJunoHub)
	require.NoError(t, err)

	t.Cleanup(
		func() {
			err := r.StopRelayer(ctx, eRep)
			if err != nil {
				// do stuff
			}
			for _, c := range chains {
				if err = c.Cleanup(ctx); err != nil {
					// do stuff
				}
			}
		},
	)

	// Get original account balances
	osmosisUser := users[0]
	gaiaUser := users[1]
	junoUser := users[2]

	osmosisBalOG, err := osmosis.GetBalance(ctx, osmosisUser.Bech32Address(osmosis.Config().Bech32Prefix), osmosis.Config().Denom)
	require.NoError(t, err)

	// Send packet from Osmosis->Hub->Juno
	// receiver format: {intermediate_refund_address}|{foward_port}/{forward_channel}:{final_destination_address}
	const transferAmount = 100000
	receiver := fmt.Sprintf("%s|%s/%s:%s", gaiaUser.Bech32Address(gaia.Config().Bech32Prefix), channels[1].PortID, channels[1].ChannelID, junoUser.Bech32Address(juno.Config().Bech32Prefix))
	transfer := ibc.WalletAmount{
		Address: receiver,
		Denom:   osmosis.Config().Denom,
		Amount:  transferAmount,
	}

	_, err = osmosis.SendIBCTransfer(ctx, channels[0].ChannelID, osmosisUser.KeyName, transfer, nil)
	require.NoError(t, err)

	// Wait for transfer to be relayed
	err = test.WaitForBlocks(ctx, 20, gaia)
	require.NoError(t, err)

	// Check that the funds sent are gone from the acc on osmosis
	osmosisBal, err := osmosis.GetBalance(ctx, osmosisUser.Bech32Address(osmosis.Config().Bech32Prefix), osmosis.Config().Denom)
	require.NoError(t, err)
	require.Equal(t, osmosisBalOG-transferAmount, osmosisBal)

	srcDenomTrace := transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom(channels[0].PortID, channels[0].ChannelID, osmosis.Config().Denom))
	dstIbcDenom := srcDenomTrace.IBCDenom()

	srcDenomTrace = transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom(channels[1].PortID, channels[1].ChannelID, srcDenomTrace.String()))
	dstIbcDenom = srcDenomTrace.IBCDenom()

	// Check that the funds sent are present in the acc on juno
	junoBal, err := juno.GetBalance(ctx, junoUser.Bech32Address(juno.Config().Bech32Prefix), dstIbcDenom)
	require.NoError(t, err)
	require.Equal(t, transferAmount, junoBal)

	// Send packet back from Juno->Hub->Osmosis
	//receiver = fmt.Sprintf("%s|%s/%s:%s", gaiaUser.Bech32Address(gaia.Config().Denom), channels[0].PortID, channels[0].ChannelID, osmosisUser.Bech32Address(osmosis.Config().Denom))
	//transfer = ibc.WalletAmount{
	//	Address: receiver,
	//	Denom:   osmosis.Config().Denom,
	//	Amount:  100000,
	//}
	//_, err = osmosis.SendIBCTransfer(ctx, channels[0].ChannelID, junoUser.KeyName, transfer, nil)
	//require.NoError(t, err)

	// Wait for transfer to be relayed
	//err = test.WaitForBlocks(ctx, 10, gaia)
	//require.NoError(t, err)

	// Send a malformed packet with invalid receiver address from Osmosis->Hub->Juno
}
