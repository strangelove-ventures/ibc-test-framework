package cosmos_test

import (
	"context"
	"cosmossdk.io/math"
	"encoding/base64"
	"fmt"
	"github.com/avast/retry-go/v4"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/privval"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/strangelove-ventures/interchaintest/v8"
	"github.com/strangelove-ventures/interchaintest/v8/chain/cosmos"
	"github.com/strangelove-ventures/interchaintest/v8/ibc"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"strings"
	"testing"
	"time"
)

var sdk50Genesis = []cosmos.GenesisKV{
	{
		Key:   "app_state.gov.params.voting_period",
		Value: votingPeriod,
	},
	{
		Key:   "app_state.gov.params.max_deposit_period",
		Value: maxDepositPeriod,
	},
	{
		Key:   "app_state.gov.params.min_deposit.0.denom",
		Value: "stake",
	},
	{
		Key:   "app_state.gov.params.min_deposit.0.amount",
		Value: "1",
	},
}

func TestRollkitCelestiaDevnet(t *testing.T) {
	ctx := context.Background()
	var rollkitChain *cosmos.CosmosChain

	cf := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
		{
			Name:      "gm",
			ChainName: "gm",
			Version:   "tutorial-local-da",
			ChainConfig: ibc.ChainConfig{
				Type:    "cosmos",
				Name:    "gm",
				ChainID: "gm",
				Images: []ibc.DockerImage{
					{
						Repository: "ghcr.io/gjermundgaraba/gm",
						Version:    "tutorial-local-da",
						UidGid:     "10001:10001",
					},
				},
				Bin:              "gmd",
				Bech32Prefix:     "gm",
				Denom:            "stake",
				CoinType:         "118",
				SigningAlgorithm: "",
				GasPrices:        "0stake",
				GasAdjustment:    2.0,
				TrustingPeriod:   "112h",
				NoHostMount:      false,
				SkipGenTx:        false,
				PreGenesis:       nil,
				ModifyGenesis: func(config ibc.ChainConfig, bytes []byte) ([]byte, error) {
					valKeyFileBz, _, err := rollkitChain.Validators[0].Exec(ctx, []string{"cat", "/var/cosmos-chain/gm/config/priv_validator_key.json"}, []string{})
					if err != nil {
						return nil, err
					}
					var pvKey privval.FilePVKey
					if err = cmtjson.Unmarshal(valKeyFileBz, &pvKey); err != nil {
						return nil, err
					}

					newGenesis := append(sdk50Genesis, cosmos.GenesisKV{
						Key: "consensus.validators",
						Value: []map[string]interface{}{
							{
								"address": pvKey.Address.String(),
								"pub_key": map[string]interface{}{
									"type":  "tendermint/PubKeyEd25519",
									"value": base64.StdEncoding.EncodeToString(pvKey.PubKey.Bytes()),
								},
								"power": "1000",              // This is important
								"name":  "Rollkit Sequencer", // This is important
							},
						},
					})

					daHostName := rollkitChain.Sidecars[0].HostName()

					if _, _, err := rollkitChain.Sidecars[0].Exec(ctx, []string{"mkdir", "/home/celestia/bridge"}, []string{}); err != nil {
						return nil, err
					}

					daAuthTokenBz, _, err := rollkitChain.Sidecars[0].Exec(ctx, []string{"celestia", "bridge", "auth", "admin", "--node.store", "/home/celestia/bridge"}, []string{})
					if err != nil {
						return nil, err
					}
					daAuthToken := strings.TrimSuffix(string(daAuthTokenBz), "\n")

					if _, _, err := rollkitChain.Validators[0].Exec(ctx, []string{"bash", "-c", fmt.Sprintf(`echo "[rollkit]
da_address = \"http://%s:%s\"
da_auth_token = \"%s\"" >> /var/cosmos-chain/gm/config/config.toml`, daHostName, "26658", daAuthToken)}, []string{}); err != nil {
						return nil, err
					}

					return cosmos.ModifyGenesis(newGenesis)(config, bytes)
				},
				ModifyGenesisAmounts: func(_ int) (sdk.Coin, sdk.Coin) {
					return sdk.NewInt64Coin("stake", 10_000_000_000_000), sdk.NewInt64Coin("stake", 1_000_000_000)
				},
				ConfigFileOverrides: nil,
				EncodingConfig:      nil,
				UsingChainIDFlagCLI: false,
				SidecarConfigs: []ibc.SidecarConfig{
					{
						ProcessName: "local-celestia-devnet",
						Image: ibc.DockerImage{
							Repository: "ghcr.io/rollkit/local-celestia-devnet",
							Version:    "latest",
							UidGid:     "1025:1025",
						},
						HomeDir: "/home/celestia",
						Ports: []string{
							"26650/tcp",
							"26657/tcp",
							"26658/tcp",
							"26659/tcp",
							"9090/tcp",
						},
						StartCmd: []string{"/bin/bash", "/opt/entrypoint.sh"},
						Env:      nil, // Here we could set CELESTIA_NAMESPACE if needed
						PreStart: true,
						StartCheck: func(index int) error {
							return retry.Do(func() error {
								daHostName := rollkitChain.Sidecars[0].HostName()
								_, errOut, err := rollkitChain.Sidecars[0].Exec(ctx, []string{"celestia-appd", "status", "--node", fmt.Sprintf("http://%s:26657", daHostName)}, []string{})
								if err != nil {
									return err
								}

								var status coretypes.ResultStatus
								if err = cmtjson.Unmarshal(errOut, &status); err != nil {
									return err
								}

								if status.SyncInfo.CatchingUp {
									return fmt.Errorf("node is still catching up")
								}

								time.Sleep(5 * time.Second) // just for good measure

								return nil
							}, retry.Context(ctx), retry.Attempts(40), retry.Delay(3*time.Second), retry.DelayType(retry.FixedDelay))
						},
						ValidatorProcess: false,
					},
				},
				AdditionalStartArgs: []string{"--rollkit.aggregator", "true", "--rollkit.da_start_height", "1", "--api.enable", "--api.enabled-unsafe-cors"},
			},
			NumValidators: &numValsOne,
			NumFullNodes:  &numFullNodesZero,
		},
	})

	chains, err := cf.Chains(t.Name())
	require.NoError(t, err)

	rollkitChain = chains[0].(*cosmos.CosmosChain)

	ic := interchaintest.NewInterchain().
		AddChain(rollkitChain)

	client, network := interchaintest.DockerSetup(t)

	require.NoError(t, ic.Build(ctx, nil, interchaintest.InterchainBuildOptions{
		TestName:         t.Name(),
		Client:           client,
		NetworkID:        network,
		SkipPathCreation: true,
	}))
	t.Cleanup(func() {
		_ = ic.Close()
	})

	// Faucet funds to a user
	users := interchaintest.GetAndFundTestUsers(t, ctx, "default", genesisFundsAmt, rollkitChain, rollkitChain)
	user := users[0]
	user2 := users[1]

	// get the users balance
	initBal, err := rollkitChain.GetBalance(ctx, user.FormattedAddress(), "stake")
	require.NoError(t, err)

	// Send many transactions in a row
	for i := 0; i < 10; i++ {
		require.NoError(t, rollkitChain.SendFunds(ctx, user.KeyName(), ibc.WalletAmount{
			Address: user2.FormattedAddress(),
			Denom:   "stake",
			Amount:  math.NewInt(1),
		}))
		require.NoError(t, rollkitChain.SendFunds(ctx, user2.KeyName(), ibc.WalletAmount{
			Address: user.FormattedAddress(),
			Denom:   "stake",
			Amount:  math.NewInt(1),
		}))
	}

	endBal, err := rollkitChain.GetBalance(ctx, user.FormattedAddress(), "stake")
	require.NoError(t, err)
	require.EqualValues(t, initBal, endBal)
}
