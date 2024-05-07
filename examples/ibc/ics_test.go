package ibc_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cosmossdk.io/math"
	transfertypes "github.com/cosmos/ibc-go/v8/modules/apps/transfer/types"
	ibcconntypes "github.com/cosmos/ibc-go/v8/modules/core/03-connection/types"
	interchaintest "github.com/strangelove-ventures/interchaintest/v8"
	"github.com/strangelove-ventures/interchaintest/v8/chain/cosmos"
	"github.com/strangelove-ventures/interchaintest/v8/ibc"
	"github.com/strangelove-ventures/interchaintest/v8/relayer"
	"github.com/strangelove-ventures/interchaintest/v8/testreporter"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

var (
	icsVersions     = []string{"v3.1.0", "v3.3.0", "v4.0.0"}
	vals            = 1
	fNodes          = 0
	providerChainID = "provider-1"
)

// This tests Cosmos Interchain Security, spinning up a provider and a single consumer chain.
// go test -timeout 3000s -run ^TestICS$ github.com/strangelove-ventures/interchaintest/v8/examples/ibc -v  -test.short
func TestICS(t *testing.T) {
	if testing.Short() {
		ver := icsVersions[0]
		t.Logf("Running in short mode, only testing the latest ICS version: %s", ver)
		icsVersions = []string{ver}
	}

	for _, version := range icsVersions {
		version := version
		testName := "ics_" + strings.ReplaceAll(version, ".", "_")

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			icsTest(t, version)
		})
	}

}

func icsTest(t *testing.T, version string) {
	ctx := context.Background()

	consumerBechPrefix := "cosmos"
	if version == "v4.0.0" {
		consumerBechPrefix = "consumer"
	}

	cf := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
		{
			Name: "ics-provider", Version: version,
			NumValidators: &vals, NumFullNodes: &fNodes,
			ChainConfig: ibc.ChainConfig{GasAdjustment: 1.5, ChainID: providerChainID, TrustingPeriod: "336h"},
		},
		{
			Name: "ics-consumer", Version: version,
			NumValidators: &vals, NumFullNodes: &fNodes,
			ChainConfig: ibc.ChainConfig{GasAdjustment: 1.5, ChainID: "consumer-1", Bech32Prefix: consumerBechPrefix},
		},
	})

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)
	provider, consumer := chains[0].(*cosmos.CosmosChain), chains[1].(*cosmos.CosmosChain)

	// Relayer Factory
	client, network := interchaintest.DockerSetup(t)
	r := interchaintest.NewBuiltinRelayerFactory(
		ibc.CosmosRly,
		zaptest.NewLogger(t),
		relayer.StartupFlags("--block-history", "100"),
	).Build(t, client, network)

	// Prep Interchain
	const ibcPath = "ics-path"
	ic := interchaintest.NewInterchain().
		AddChain(provider).
		AddChain(consumer).
		AddRelayer(r, "relayer").
		AddProviderConsumerLink(interchaintest.ProviderConsumerLink{
			Provider: provider,
			Consumer: consumer,
			Relayer:  r,
			Path:     ibcPath,
		})

	// Reporter/logs
	rep := testreporter.NewNopReporter()
	eRep := rep.RelayerExecReporter(t)

	// Build interchain
	err = ic.Build(ctx, eRep, interchaintest.InterchainBuildOptions{
		TestName:         t.Name(),
		Client:           client,
		NetworkID:        network,
		SkipPathCreation: false,
	})
	require.NoError(t, err, "failed to build interchain")

	// ------------------ ICS Setup ------------------

	// Finish the ICS provider chain initialization.
	// - Restarts the relayer to connect ics20-1 transfer channel
	// - Delegates tokens to the provider to update consensus value
	// - Flushes the IBC state to the consumer
	err = provider.FinishICSProviderSetup(ctx, r, eRep, ibcPath)
	require.NoError(t, err)

	// ------------------ Test Begins ------------------

	// Fund users
	// NOTE: this has to be done after the provider delegation & IBC update to the consumer.
	amt := math.NewInt(10_000_000)
	users := interchaintest.GetAndFundTestUsers(t, ctx, "default", amt, consumer, provider)
	consumerUser, providerUser := users[0], users[1]

	t.Run("validate consumer action executed", func(t *testing.T) {
		bal, err := consumer.BankQueryBalance(ctx, consumerUser.FormattedAddress(), consumer.Config().Denom)
		require.NoError(t, err)
		require.EqualValues(t, amt, bal)
	})

	t.Run("provider -> consumer IBC transfer", func(t *testing.T) {
		providerChannelInfo, err := r.GetChannels(ctx, eRep, provider.Config().ChainID)
		require.NoError(t, err)

		channelID, err := getTransferChannel(providerChannelInfo)
		require.NoError(t, err, providerChannelInfo)

		consumerChannelInfo, err := r.GetChannels(ctx, eRep, consumer.Config().ChainID)
		require.NoError(t, err)

		consumerChannelID, err := getTransferChannel(consumerChannelInfo)
		require.NoError(t, err, consumerChannelInfo)

		dstAddress := consumerUser.FormattedAddress()
		sendAmt := math.NewInt(7)
		transfer := ibc.WalletAmount{
			Address: dstAddress,
			Denom:   provider.Config().Denom,
			Amount:  sendAmt,
		}

		tx, err := provider.SendIBCTransfer(ctx, channelID, providerUser.KeyName(), transfer, ibc.TransferOptions{})
		require.NoError(t, err)
		require.NoError(t, tx.Validate())

		require.NoError(t, r.Flush(ctx, eRep, ibcPath, channelID))

		srcDenomTrace := transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom("transfer", consumerChannelID, provider.Config().Denom))
		dstIbcDenom := srcDenomTrace.IBCDenom()

		consumerBal, err := consumer.BankQueryBalance(ctx, consumerUser.FormattedAddress(), dstIbcDenom)
		require.NoError(t, err)
		require.EqualValues(t, sendAmt, consumerBal)
	})
}

func getTransferChannel(channels []ibc.ChannelOutput) (string, error) {
	for _, channel := range channels {
		if channel.PortID == "transfer" && channel.State == ibcconntypes.OPEN.String() {
			return channel.ChannelID, nil
		}
	}

	return "", fmt.Errorf("no open transfer channel found")
}
