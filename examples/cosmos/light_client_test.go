package cosmos_test

import (
	"context"
	"testing"

	clienttypes "github.com/cosmos/ibc-go/v6/modules/core/02-client/types"
	ibctest "github.com/strangelove-ventures/ibctest/v6"
	"github.com/strangelove-ventures/ibctest/v6/chain/cosmos"
	"github.com/strangelove-ventures/ibctest/v6/ibc"
	"github.com/strangelove-ventures/ibctest/v6/testreporter"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestUpdateLightClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	t.Parallel()

	ctx := context.Background()

	// Chains
	cf := ibctest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*ibctest.ChainSpec{
		{Name: "gaia", Version: gaiaVersion},
		{Name: "osmosis", Version: osmosisVersion},
	})

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)
	gaia, osmosis := chains[0], chains[1]

	// Relayer
	client, network := ibctest.DockerSetup(t)
	r := ibctest.NewBuiltinRelayerFactory(ibc.CosmosRly, zaptest.NewLogger(t)).Build(
		t, client, network)

	ic := ibctest.NewInterchain().
		AddChain(gaia).
		AddChain(osmosis).
		AddRelayer(r, "relayer").
		AddLink(ibctest.InterchainLink{
			Chain1:  gaia,
			Chain2:  osmosis,
			Relayer: r,
			Path:    "client-test-path",
		})

	// Build interchain
	rep := testreporter.NewNopReporter()
	eRep := rep.RelayerExecReporter(t)
	require.NoError(t, ic.Build(ctx, eRep, ibctest.InterchainBuildOptions{
		TestName:  t.Name(),
		Client:    client,
		NetworkID: network,
	}))
	t.Cleanup(func() {
		_ = ic.Close()
	})

	require.NoError(t, r.StartRelayer(ctx, eRep))
	t.Cleanup(func() {
		_ = r.StopRelayer(ctx, eRep)
	})

	// Create and Fund User Wallets
	fundAmount := int64(10_000_000)
	users := ibctest.GetAndFundTestUsers(t, ctx, "default", fundAmount, gaia, osmosis)
	gaiaUser, osmoUser := users[0], users[1]

	// Get Channel ID
	gaiaChannelInfo, err := r.GetChannels(ctx, eRep, gaia.Config().ChainID)
	require.NoError(t, err)
	chanID := gaiaChannelInfo[0].ChannelID

	height, err := osmosis.Height(ctx)
	require.NoError(t, err)

	amountToSend := int64(553255) // Unique amount to make log searching easier.
	dstAddress := osmoUser.GetFormattedAddress(osmosis.Config().Bech32Prefix)
	transfer := ibc.WalletAmount{
		Address: dstAddress,
		Denom:   gaia.Config().Denom,
		Amount:  amountToSend,
	}
	tx, err := gaia.SendIBCTransfer(ctx, chanID, gaiaUser.GetKeyName(), transfer, ibc.TransferOptions{})
	require.NoError(t, err)
	require.NoError(t, tx.Validate())

	chain := osmosis.(*cosmos.CosmosChain)
	reg := chain.Config().EncodingConfig.InterfaceRegistry
	msg, err := cosmos.PollForMessage[*clienttypes.MsgUpdateClient](ctx, chain, reg, height, height+10, nil)
	require.NoError(t, err)

	require.Equal(t, "07-tendermint-0", msg.ClientId)
	require.NotEmpty(t, msg.Signer)
	// TODO: Assert header information
}
