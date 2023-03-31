package cosmos_test

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/strangelove-ventures/interchaintest/v3"
	"github.com/strangelove-ventures/interchaintest/v3/chain/cosmos"
	"github.com/strangelove-ventures/interchaintest/v3/ibc"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestJunoParamChange(t *testing.T) {
	CosmosChainParamChangeTest(t, "juno", "v13.0.1")
}

func CosmosChainParamChangeTest(t *testing.T, name, version string) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	t.Parallel()

	numVals := 1
	numFullNodes := 1

	cf := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
		{
			Name:      name,
			ChainName: name,
			Version:   version,
			ChainConfig: ibc.ChainConfig{
				Denom:         "ujuno",
				ModifyGenesis: cosmos.ModifyGenesisProposalTime(votingPeriod, maxDepositPeriod),
			},
			NumValidators: &numVals,
			NumFullNodes:  &numFullNodes,
		},
	})

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)

	chain := chains[0].(*cosmos.CosmosChain)

	ic := interchaintest.NewInterchain().
		AddChain(chain)

	ctx := context.Background()
	client, network := interchaintest.DockerSetup(t)

	require.NoError(t, ic.Build(ctx, nil, interchaintest.InterchainBuildOptions{
		TestName:  t.Name(),
		Client:    client,
		NetworkID: network,
		// BlockDatabaseFile: interchaintest.DefaultBlockDatabaseFilepath(),
		SkipPathCreation: true,
	}))
	t.Cleanup(func() {
		_ = ic.Close()
	})

	const userFunds = int64(10_000_000_000)
	users := interchaintest.GetAndFundTestUsers(t, ctx, t.Name(), userFunds, chain)
	chainUser := users[0]

	param, _ := chain.QueryParam(ctx, "staking", "MaxValidators")
	require.Equal(t, "100", param.Value, "MaxValidators value is not 100")

	current_directory, _ := os.Getwd()
	param_change_path := path.Join(current_directory, "params", "IncreaseValidatorsParam.json")

	paramTx, err := chain.ParamChangeProposal(ctx, chainUser.KeyName, param_change_path)
	require.NoError(t, err, "error submitting param change proposal tx")

	err = chain.VoteOnProposalAllValidators(ctx, paramTx.ProposalID, cosmos.ProposalVoteYes)
	require.NoError(t, err, "failed to submit votes")

	height, _ := chain.Height(ctx)
	_, err = cosmos.PollForProposalStatus(ctx, chain, height, height+10, paramTx.ProposalID, cosmos.ProposalStatusPassed)
	require.NoError(t, err, "proposal status did not change to passed in expected number of blocks")

	param, _ = chain.QueryParam(ctx, "staking", "MaxValidators")
	require.Equal(t, "110", param.Value, "MaxValidators value is not 110")
}
