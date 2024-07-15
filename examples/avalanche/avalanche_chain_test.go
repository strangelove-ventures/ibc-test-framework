package avalanche_test

import (
	"context"
	"testing"

	"cosmossdk.io/math"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"

	"github.com/strangelove-ventures/interchaintest/v8"
	subnetevm "github.com/strangelove-ventures/interchaintest/v8/examples/avalanche/subnet-evm"
	"github.com/strangelove-ventures/interchaintest/v8/ibc"
	"github.com/strangelove-ventures/interchaintest/v8/testutil"
)

func TestAvalanche(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	t.Parallel()
	client, network := interchaintest.DockerSetup(t)

	nv := 5
	nf := 0

	chains, err := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
		{
			Name:    "avalanche",
			Version: "v1.10.18-ibc",
			ChainConfig: ibc.ChainConfig{
				ChainID: "1337",
				Images: []ibc.DockerImage{
					{
						Repository: "avalanchego",
						Version:    "v1.10.18-ibc",
						UidGid:     "1025:1025",
					},
				},
				AvalancheSubnets: []ibc.AvalancheSubnetConfig{
					{
						Name:                "subnetevm",
						Genesis:             subnetevm.Genesis,
						SubnetClientFactory: subnetevm.NewSubnetEvmClient,
					},
				},
			},
			NumFullNodes:  &nf,
			NumValidators: &nv,
		},
	},
	).Chains(t.Name())

	require.NoError(t, err, "failed to get avalanche chain")
	require.Len(t, chains, 1)

	chain := chains[0]

	ctx := context.Background()

	err = chain.Initialize(ctx, t.Name(), client, network)
	require.NoError(t, err, "failed to initialize avalanche chain")

	err = chain.Start(t.Name(), ctx)
	require.NoError(t, err, "failed to start avalanche chain")

	subnetCtx := context.WithValue(ctx, "subnet", "0")

	eg := new(errgroup.Group)
	eg.Go(func() error {
		err := chain.SendFunds(subnetCtx, "56289e99c94b6912bfc12adc093c9b51124f0dc54ac7a766b2bc5ccf558d8027", ibc.WalletAmount{
			Address: "0x8db97C7cEcE249c2b98bDC0226Cc4C2A57BF52FC",
			Amount:  math.NewInt(1000000),
		})
		if err != nil {
			return err
		}
		return chain.SendFunds(subnetCtx, "56289e99c94b6912bfc12adc093c9b51124f0dc54ac7a766b2bc5ccf558d8027", ibc.WalletAmount{
			Address: "0x8db97C7cEcE249c2b98bDC0226Cc4C2A57BF52FD",
			Amount:  math.NewInt(2000000),
		})
	})
	eg.Go(func() error {
		return testutil.WaitForBlocks(subnetCtx, 1, chain)
	})

	require.NoError(t, eg.Wait(), "avalanche chain failed to make blocks")
}
