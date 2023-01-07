package conformance

import (
	"context"
	"fmt"
	"testing"

	"github.com/cosmos/cosmos-sdk/types"
	ibctest "github.com/strangelove-ventures/ibctest/v3"
	"github.com/strangelove-ventures/ibctest/v3/ibc"
	"github.com/strangelove-ventures/ibctest/v3/relayer"
	"github.com/strangelove-ventures/ibctest/v3/testreporter"
	"github.com/strangelove-ventures/ibctest/v3/testutil"
	"github.com/stretchr/testify/require"
)

func TestRelayerFlushing(t *testing.T, ctx context.Context, cf ibctest.ChainFactory, rf ibctest.RelayerFactory, rep *testreporter.Reporter) {
	rep.TrackTest(t)

	// FlushPackets will be exercised in a subtest,
	// but check that capability first in case we can avoid setup.
	requireCapabilities(t, rep, rf, relayer.FlushPackets)

	client, network := ibctest.DockerSetup(t)

	req := require.New(rep.TestifyT(t))
	chains, err := cf.Chains(t.Name())
	req.NoError(err, "failed to get chains")

	if len(chains) != 2 {
		panic(fmt.Errorf("expected 2 chains, got %d", len(chains)))
	}

	c0, c1 := chains[0], chains[1]

	r := rf.Build(t, client, network)

	const pathName = "p"
	ic := ibctest.NewInterchain().
		AddChain(c0).
		AddChain(c1).
		AddRelayer(r, "r").
		AddLink(ibctest.InterchainLink{
			Chain1:  c0,
			Chain2:  c1,
			Relayer: r,

			Path:              pathName,
			CreateChannelOpts: ibc.DefaultChannelOpts(),
		})

	eRep := rep.RelayerExecReporter(t)

	req.NoError(ic.Build(ctx, eRep, ibctest.InterchainBuildOptions{
		TestName:  t.Name(),
		Client:    client,
		NetworkID: network,
	}))
	defer ic.Close()

	// Get faucet address on destination chain for ibc transfer.
	c1FaucetAddrBytes, err := c1.GetAddress(ctx, ibctest.FaucetAccountKeyName)
	req.NoError(err)
	c1FaucetAddr, err := types.Bech32ifyAddressBytes(c1.Config().Bech32Prefix, c1FaucetAddrBytes)
	req.NoError(err)

	channels, err := r.GetChannels(ctx, eRep, c0.Config().ChainID)
	req.NoError(err)
	req.Len(channels, 1)

	c0ChannelID := channels[0].ChannelID
	c1ChannelID := channels[0].Counterparty.ChannelID

	beforeTransferHeight, err := c0.Height(ctx)
	req.NoError(err)

	const txAmount = 112233 // Arbitrary amount that is easy to find in logs.
	tx, err := c0.SendIBCTransfer(ctx, c0ChannelID, ibctest.FaucetAccountKeyName, ibc.WalletAmount{
		Address: c1FaucetAddr,
		Denom:   c0.Config().Denom,
		Amount:  txAmount,
	}, ibc.TransferOptions{})
	req.NoError(err)
	req.NoError(tx.Validate())

	t.Run("flush packets", func(t *testing.T) {
		rep.TrackTest(t)

		eRep := rep.RelayerExecReporter(t)

		req := require.New(rep.TestifyT(t))

		// Should trigger MsgRecvPacket.
		req.NoError(r.FlushPackets(ctx, eRep, pathName, c0ChannelID))

		req.NoError(testutil.WaitForBlocks(ctx, 3, c0, c1))

		req.NoError(r.FlushPackets(ctx, eRep, pathName, c1ChannelID))

		afterFlushHeight, err := c0.Height(ctx)
		req.NoError(err)

		// Ack shouldn't happen yet.
		_, err = testutil.PollForAck(ctx, c0, beforeTransferHeight, afterFlushHeight+2, tx.Packet)
		req.ErrorIs(err, testutil.ErrNotFound)
	})

	t.Run("flush acks", func(t *testing.T) {
		rep.TrackTest(t)
		requireCapabilities(t, rep, rf, relayer.FlushAcknowledgements)

		eRep := rep.RelayerExecReporter(t)

		req := require.New(rep.TestifyT(t))
		req.NoError(r.FlushAcknowledgements(ctx, eRep, pathName, c0ChannelID))

		afterFlushHeight, err := c0.Height(ctx)
		req.NoError(err)

		// Now the ack must be present.
		_, err = testutil.PollForAck(ctx, c0, beforeTransferHeight, afterFlushHeight+2, tx.Packet)
		req.NoError(err)
	})
}
