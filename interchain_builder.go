package interchaintest

import (
	"context"
	"testing"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/types/module/testutil"
	"github.com/docker/docker/client"
	"github.com/strangelove-ventures/interchaintest/v8/chain/cosmos"
	"github.com/strangelove-ventures/interchaintest/v8/ibc"
	"github.com/strangelove-ventures/interchaintest/v8/testreporter"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type codecRegistry func(registry codectypes.InterfaceRegistry)

// RegisterInterfaces registers the interfaces for the input codec register functions.
func RegisterInterfaces(codecIR ...codecRegistry) *testutil.TestEncodingConfig {
	cfg := cosmos.DefaultEncoding()
	for _, r := range codecIR {
		r(cfg.InterfaceRegistry)
	}
	return &cfg
}

// CreateChainWithConfig builds a single chain from the given ibc config.
func CreateChainWithConfig(t *testing.T, numVals, numFull int, name, version string, config ibc.ChainConfig) []ibc.Chain {
	if version == "" {
		if len(config.Images) == 0 {
			version = "latest"
			t.Logf("no image version specified in config, using %s", version)
		} else {
			version = config.Images[0].Version
		}
	}

	cf := NewBuiltinChainFactory(zaptest.NewLogger(t), []*ChainSpec{
		{
			Name:          name,
			ChainName:     name,
			Version:       version,
			ChainConfig:   config,
			NumValidators: &numVals,
			NumFullNodes:  &numFull,
		},
	})

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)

	return chains
}

// CreateChainsWithChainSpecs builds multiple chains from the given chain specs.
func CreateChainsWithChainSpecs(t *testing.T, chainSpecs []*ChainSpec) []ibc.Chain {
	cf := NewBuiltinChainFactory(zaptest.NewLogger(t), chainSpecs)

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)

	return chains
}

// TODO: Add simple relayer support.
func BuildInitialChain(t *testing.T, chains []ibc.Chain, enableBlockDB bool) (*Interchain, context.Context, *client.Client, string) {
	// Create a new Interchain object which describes the chains, relayers, and IBC connections we want to use
	ic := NewInterchain()

	for _, chain := range chains {
		ic = ic.AddChain(chain)
	}

	rep := testreporter.NewNopReporter()
	eRep := rep.RelayerExecReporter(t)

	ctx := context.Background()
	client, network := DockerSetup(t)

	opt := InterchainBuildOptions{
		TestName:         t.Name(),
		Client:           client,
		NetworkID:        network,
		SkipPathCreation: true,
	}
	if enableBlockDB {
		// This can be used to write to the block database which will index all block data e.g. txs, msgs, events, etc.
		opt.BlockDatabaseFile = DefaultBlockDatabaseFilepath()
	}

	err := ic.Build(ctx, eRep, opt)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = ic.Close()
	})

	return ic, ctx, client, network
}
