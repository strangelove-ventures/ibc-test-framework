package ibctest_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/strangelove-ventures/ibctest"
	"github.com/strangelove-ventures/ibctest/ibc"
	"github.com/strangelove-ventures/ibctest/relayer/rly"
	"github.com/strangelove-ventures/ibctest/testreporter"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestInterchain_DuplicateChain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	t.Parallel()

	home := t.TempDir()
	pool, network := ibctest.DockerSetup(t)

	cf := ibctest.NewBuiltinChainFactory([]ibctest.BuiltinChainFactoryEntry{
		// Two otherwise identical chains that only differ by ChainID.
		{Name: "gaia", NameOverride: "g1", Version: "v7.0.1", ChainID: "cosmoshub-0", NumValidators: 2, NumFullNodes: 1},
		{Name: "gaia", NameOverride: "g2", Version: "v7.0.1", ChainID: "cosmoshub-1", NumValidators: 2, NumFullNodes: 1},
	}, zaptest.NewLogger(t))

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)

	gaia0, gaia1 := chains[0], chains[1]

	r := ibctest.NewBuiltinRelayerFactory(ibc.CosmosRly, zaptest.NewLogger(t)).Build(
		t, pool, network, home,
	)

	ic := ibctest.NewInterchain().
		AddChain(gaia0).
		AddChain(gaia1).
		AddRelayer(r, "r").
		AddLink(ibctest.InterchainLink{
			Chain1:  gaia0,
			Chain2:  gaia1,
			Relayer: r,
		})

	rep := testreporter.NewNopReporter()
	eRep := rep.RelayerExecReporter(t)

	ctx := context.Background()
	require.NoError(t, ic.Build(ctx, eRep, ibctest.InterchainBuildOptions{
		TestName:  t.Name(),
		HomeDir:   home,
		Pool:      pool,
		NetworkID: network,

		SkipPathCreation: true,
	}))
}

func TestInterchain_ConflictRejection(t *testing.T) {
	t.Run("duplicate chain", func(t *testing.T) {
		cf := ibctest.NewBuiltinChainFactory([]ibctest.BuiltinChainFactoryEntry{
			{Name: "gaia", Version: "v7.0.1", ChainID: "cosmoshub-0", NumValidators: 2, NumFullNodes: 1},
		}, zap.NewNop())

		chains, err := cf.Chains(t.Name())
		require.NoError(t, err)
		chain := chains[0]

		exp := fmt.Sprintf("chain %v was already added", chain)
		require.PanicsWithError(t, exp, func() {
			_ = ibctest.NewInterchain().AddChain(chain).AddChain(chain)
		})
	})

	t.Run("chain name", func(t *testing.T) {
		cf := ibctest.NewBuiltinChainFactory([]ibctest.BuiltinChainFactoryEntry{
			// Different ChainID, but no NameOverride supplied.
			{Name: "gaia", Version: "v7.0.1", ChainID: "cosmoshub-0", NumValidators: 2, NumFullNodes: 1},
			{Name: "gaia", Version: "v7.0.1", ChainID: "cosmoshub-1", NumValidators: 2, NumFullNodes: 1},
		}, zap.NewNop())

		chains, err := cf.Chains(t.Name())
		require.NoError(t, err)

		require.PanicsWithError(t, "a chain with name gaia already exists", func() {
			_ = ibctest.NewInterchain().AddChain(chains[0]).AddChain(chains[1])
		})
	})

	t.Run("chain ID", func(t *testing.T) {
		cf := ibctest.NewBuiltinChainFactory([]ibctest.BuiltinChainFactoryEntry{
			// Valid NameOverride but duplicate ChainID.
			{Name: "gaia", NameOverride: "g1", Version: "v7.0.1", ChainID: "cosmoshub-0", NumValidators: 2, NumFullNodes: 1},
			{Name: "gaia", NameOverride: "g2", Version: "v7.0.1", ChainID: "cosmoshub-0", NumValidators: 2, NumFullNodes: 1},
		}, zap.NewNop())

		chains, err := cf.Chains(t.Name())
		require.NoError(t, err)

		require.PanicsWithError(t, "a chain with ID cosmoshub-0 already exists", func() {
			_ = ibctest.NewInterchain().AddChain(chains[0]).AddChain(chains[1])
		})
	})

	t.Run("duplicate relayer", func(t *testing.T) {
		var r rly.CosmosRelayer

		exp := fmt.Sprintf("relayer %v was already added", &r)
		require.PanicsWithError(t, exp, func() {
			_ = ibctest.NewInterchain().AddRelayer(&r, "r1").AddRelayer(&r, "r2")
		})
	})

	t.Run("relayer name", func(t *testing.T) {
		var r1, r2 rly.CosmosRelayer

		require.PanicsWithError(t, "a relayer with name r already exists", func() {
			_ = ibctest.NewInterchain().AddRelayer(&r1, "r").AddRelayer(&r2, "r")
		})
	})
}

func TestInterchain_AddNil(t *testing.T) {
	require.PanicsWithError(t, "cannot add nil chain", func() {
		_ = ibctest.NewInterchain().AddChain(nil)
	})

	require.PanicsWithError(t, "cannot add nil relayer", func() {
		_ = ibctest.NewInterchain().AddRelayer(nil, "r")
	})
}
