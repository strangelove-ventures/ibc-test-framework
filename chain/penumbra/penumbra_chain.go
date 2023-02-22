package penumbra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	"github.com/docker/docker/api/types"
	volumetypes "github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/strangelove-ventures/interchaintest/v7/chain/internal/tendermint"
	"github.com/strangelove-ventures/interchaintest/v7/ibc"
	"github.com/strangelove-ventures/interchaintest/v7/internal/dockerutil"
	"github.com/strangelove-ventures/interchaintest/v7/testutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Node struct {
	TendermintNode *tendermint.Node
	AppNode        *AppNode
}

type Nodes []Node

type Chain struct {
	log           *zap.Logger
	testName      string
	cfg           ibc.ChainConfig
	numValidators int
	numFullNodes  int
	PenumbraNodes Nodes
	keyring       keyring.Keyring
}

type ValidatorDefinition struct {
	IdentityKey    string                   `json:"identity_key"`
	ConsensusKey   string                   `json:"consensus_key"`
	Name           string                   `json:"name"`
	Website        string                   `json:"website"`
	Description    string                   `json:"description"`
	FundingStreams []ValidatorFundingStream `json:"funding_streams"`
	SequenceNumber int64                    `json:"sequence_number"`
}

type ValidatorFundingStream struct {
	Address string `json:"address"`
	RateBPS int64  `json:"rate_bps"`
}

type GenesisAppStateAllocation struct {
	Amount  int64  `json:"amount"`
	Denom   string `json:"denom"`
	Address string `json:"address"`
}

func NewChain(log *zap.Logger, testName string, chainConfig ibc.ChainConfig, numValidators int, numFullNodes int) *Chain {
	registry := codectypes.NewInterfaceRegistry()
	cryptocodec.RegisterInterfaces(registry)
	cdc := codec.NewProtoCodec(registry)
	kr := keyring.NewInMemory(cdc)

	return &Chain{
		log:           log,
		testName:      testName,
		cfg:           chainConfig,
		numValidators: numValidators,
		numFullNodes:  numFullNodes,
		keyring:       kr,
	}
}

func (c *Chain) Acknowledgements(ctx context.Context, height uint64) ([]ibc.PacketAcknowledgement, error) {
	panic("implement me")
}

func (c *Chain) Timeouts(ctx context.Context, height uint64) ([]ibc.PacketTimeout, error) {
	panic("implement me")
}

// Implements Chain interface
func (c *Chain) Config() ibc.ChainConfig {
	return c.cfg
}

// Implements Chain interface
func (c *Chain) Initialize(ctx context.Context, testName string, cli *client.Client, networkID string) error {
	return c.initializeChainNodes(ctx, testName, cli, networkID)
}

// Exec implements chain interface.
func (c *Chain) Exec(ctx context.Context, cmd []string, env []string) (stdout, stderr []byte, err error) {
	return c.getRelayerNode().AppNode.Exec(ctx, cmd, env)
}

func (c *Chain) getRelayerNode() Node {
	if len(c.PenumbraNodes) > c.numValidators {
		// use first full node
		return c.PenumbraNodes[c.numValidators]
	}
	// use first validator
	return c.PenumbraNodes[0]
}

// Implements Chain interface
func (c *Chain) GetRPCAddress() string {
	return fmt.Sprintf("http://%s:26657", c.getRelayerNode().TendermintNode.HostName())
}

// Implements Chain interface
func (c *Chain) GetGRPCAddress() string {
	return fmt.Sprintf("%s:9090", c.getRelayerNode().TendermintNode.HostName())
}

// GetHostRPCAddress returns the address of the RPC server accessible by the host.
// This will not return a valid address until the chain has been started.
func (c *Chain) GetHostRPCAddress() string {
	return "http://" + c.getRelayerNode().AppNode.hostRPCPort
}

// GetHostGRPCAddress returns the address of the gRPC server accessible by the host.
// This will not return a valid address until the chain has been started.
func (c *Chain) GetHostGRPCAddress() string {
	return c.getRelayerNode().AppNode.hostGRPCPort
}

func (c *Chain) HomeDir() string {
	panic(errors.New("HomeDir not implemented yet"))
}

// Implements Chain interface
func (c *Chain) CreateKey(ctx context.Context, keyName string) error {
	return c.getRelayerNode().AppNode.CreateKey(ctx, keyName)
}

func (c *Chain) RecoverKey(ctx context.Context, name, mnemonic string) error {
	return c.getRelayerNode().AppNode.RecoverKey(ctx, name, mnemonic)
}

// Implements Chain interface
func (c *Chain) GetAddress(ctx context.Context, keyName string) ([]byte, error) {
	return c.getRelayerNode().AppNode.GetAddress(ctx, keyName)
}

// BuildWallet will return a Penumbra wallet
// If mnemonic != "", it will restore using that mnemonic
// If mnemonic == "", it will create a new key
func (c *Chain) BuildWallet(ctx context.Context, keyName string, mnemonic string) (ibc.Wallet, error) {
	if mnemonic != "" {
		if err := c.RecoverKey(ctx, keyName, mnemonic); err != nil {
			return nil, fmt.Errorf("failed to recover key with name %q on chain %s: %w", keyName, c.cfg.Name, err)
		}
	} else {
		if err := c.CreateKey(ctx, keyName); err != nil {
			return nil, fmt.Errorf("failed to create key with name %q on chain %s: %w", keyName, c.cfg.Name, err)
		}
	}

	addrBytes, err := c.GetAddress(ctx, keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to get account address for key %q on chain %s: %w", keyName, c.cfg.Name, err)
	}

	return NewWallet(keyName, addrBytes, mnemonic, c.cfg), nil
}

// BuildRelayerWallet will return a Penumbra wallet populated with the mnemonic so that the wallet can
// be restored in the relayer node using the mnemonic. After it is built, that address is included in
// genesis with some funds.
func (c *Chain) BuildRelayerWallet(ctx context.Context, keyName string) (ibc.Wallet, error) {
	coinType, err := strconv.ParseUint(c.cfg.CoinType, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid coin type: %w", err)
	}

	info, mnemonic, err := c.keyring.NewMnemonic(
		keyName,
		keyring.English,
		hd.CreateHDPath(uint32(coinType), 0, 0).String(),
		"", // Empty passphrase.
		hd.Secp256k1,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create mnemonic: %w", err)
	}

	addrBytes, err := info.GetAddress()
	if err != nil {
		return nil, fmt.Errorf("failed to get address: %w", err)
	}

	return NewWallet(keyName, addrBytes, mnemonic, c.cfg), nil
}

// Implements Chain interface
func (c *Chain) SendFunds(ctx context.Context, keyName string, amount ibc.WalletAmount) error {
	return c.getRelayerNode().AppNode.SendFunds(ctx, keyName, amount)
}

// Implements Chain interface
func (c *Chain) SendIBCTransfer(
	ctx context.Context,
	channelID string,
	keyName string,
	amount ibc.WalletAmount,
	options ibc.TransferOptions,
) (ibc.Tx, error) {
	return c.getRelayerNode().AppNode.SendIBCTransfer(ctx, channelID, keyName, amount, options)
}

// Implements Chain interface
func (c *Chain) ExportState(ctx context.Context, height int64) (string, error) {
	panic("implement me")
}

func (c *Chain) Height(ctx context.Context) (uint64, error) {
	return c.getRelayerNode().TendermintNode.Height(ctx)
}

// Implements Chain interface
func (c *Chain) GetBalance(ctx context.Context, address string, denom string) (int64, error) {
	panic("implement me")
}

// Implements Chain interface
func (c *Chain) GetGasFeesInNativeDenom(gasPaid int64) int64 {
	gasPrice, _ := strconv.ParseFloat(strings.Replace(c.cfg.GasPrices, c.cfg.Denom, "", 1), 64)
	fees := float64(gasPaid) * gasPrice
	return int64(fees)
}

// creates the test node objects required for bootstrapping tests
func (c *Chain) initializeChainNodes(
	ctx context.Context,
	testName string,
	cli *client.Client,
	networkID string,
) error {
	penumbraNodes := []Node{}
	count := c.numValidators + c.numFullNodes
	chainCfg := c.Config()
	for _, image := range chainCfg.Images {
		rc, err := cli.ImagePull(
			ctx,
			image.Repository+":"+image.Version,
			types.ImagePullOptions{},
		)
		if err != nil {
			c.log.Error("Failed to pull image",
				zap.Error(err),
				zap.String("repository", image.Repository),
				zap.String("tag", image.Version),
			)
		} else {
			_, _ = io.Copy(io.Discard, rc)
			_ = rc.Close()
		}
	}
	for i := 0; i < count; i++ {
		tn := &tendermint.Node{
			Log: c.log, Index: i, Chain: c,
			DockerClient: cli, NetworkID: networkID, TestName: testName, Image: chainCfg.Images[0],
		}

		tv, err := cli.VolumeCreate(ctx, volumetypes.VolumeCreateBody{
			Labels: map[string]string{
				dockerutil.CleanupLabel: testName,

				dockerutil.NodeOwnerLabel: tn.Name(),
			},
		})
		if err != nil {
			return fmt.Errorf("creating tendermint volume: %w", err)
		}
		tn.VolumeName = tv.Name
		if err := dockerutil.SetVolumeOwner(ctx, dockerutil.VolumeOwnerOptions{
			Log: c.log,

			Client: cli,

			VolumeName: tn.VolumeName,
			ImageRef:   tn.Image.Ref(),
			TestName:   tn.TestName,
			UidGid:     tn.Image.UidGid,
		}); err != nil {
			return fmt.Errorf("set tendermint volume owner: %w", err)
		}

		pn := &AppNode{
			log: c.log, Index: i, Chain: c,
			DockerClient: cli, NetworkID: networkID, TestName: testName, Image: chainCfg.Images[1],
		}
		pv, err := cli.VolumeCreate(ctx, volumetypes.VolumeCreateBody{
			Labels: map[string]string{
				dockerutil.CleanupLabel: testName,

				dockerutil.NodeOwnerLabel: pn.Name(),
			},
		})
		if err != nil {
			return fmt.Errorf("creating penumbra volume: %w", err)
		}
		pn.VolumeName = pv.Name
		if err := dockerutil.SetVolumeOwner(ctx, dockerutil.VolumeOwnerOptions{
			Log: c.log,

			Client: cli,

			VolumeName: pn.VolumeName,
			ImageRef:   pn.Image.Ref(),
			TestName:   pn.TestName,
			UidGid:     tn.Image.UidGid,
		}); err != nil {
			return fmt.Errorf("set penumbra volume owner: %w", err)
		}

		penumbraNodes = append(penumbraNodes, Node{TendermintNode: tn, AppNode: pn})
	}
	c.PenumbraNodes = penumbraNodes

	return nil
}

type GenesisValidatorPubKey struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}
type GenesisValidators struct {
	Address string                 `json:"address"`
	Name    string                 `json:"name"`
	Power   string                 `json:"power"`
	PubKey  GenesisValidatorPubKey `json:"pub_key"`
}
type GenesisFile struct {
	Validators []GenesisValidators `json:"validators"`
}

type ValidatorWithIntPower struct {
	Address      string
	Power        int64
	PubKeyBase64 string
}

func (c *Chain) Start(testName string, ctx context.Context, additionalGenesisWallets ...ibc.WalletAmount) error {
	validators := c.PenumbraNodes[:c.numValidators]
	fullnodes := c.PenumbraNodes[c.numValidators:]

	chainCfg := c.Config()

	validatorDefinitions := make([]ValidatorDefinition, len(validators))
	allocations := make([]GenesisAppStateAllocation, len(validators)*2)

	eg, egCtx := errgroup.WithContext(ctx)
	for i, v := range validators {
		v := v
		i := i
		eg.Go(func() error {
			if err := v.TendermintNode.InitValidatorFiles(egCtx); err != nil {
				return fmt.Errorf("error initializing validator files: %v", err)
			}
			fr := dockerutil.NewFileRetriever(c.log, v.TendermintNode.DockerClient, v.TendermintNode.TestName)
			privValKeyBytes, err := fr.SingleFileContent(egCtx, v.TendermintNode.VolumeName, "config/priv_validator_key.json")
			if err != nil {
				return fmt.Errorf("error reading tendermint privval key file: %v", err)
			}
			privValKey := tendermint.PrivValidatorKeyFile{}
			if err := json.Unmarshal(privValKeyBytes, &privValKey); err != nil {
				return fmt.Errorf("error unmarshaling tendermint privval key: %v", err)
			}
			if err := v.AppNode.CreateKey(egCtx, valKey); err != nil {
				return fmt.Errorf("error generating wallet on penumbra node: %v", err)
			}
			if err := v.AppNode.InitValidatorFile(egCtx, valKey); err != nil {
				return fmt.Errorf("error initializing validator template on penumbra node: %v", err)
			}

			// In all likelihood, the AppNode and TendermintNode have the same DockerClient and TestName,
			// but instantiate a new FileRetriever to be defensive.
			fr = dockerutil.NewFileRetriever(c.log, v.AppNode.DockerClient, v.AppNode.TestName)
			validatorTemplateDefinitionFileBytes, err := fr.SingleFileContent(egCtx, v.AppNode.VolumeName, "validator.json")
			if err != nil {
				return fmt.Errorf("error reading validator definition template file: %v", err)
			}
			validatorTemplateDefinition := ValidatorDefinition{}
			if err := json.Unmarshal(validatorTemplateDefinitionFileBytes, &validatorTemplateDefinition); err != nil {
				return fmt.Errorf("error unmarshaling validator definition template key: %v", err)
			}
			validatorTemplateDefinition.ConsensusKey = privValKey.PubKey.Value
			validatorTemplateDefinition.Name = fmt.Sprintf("validator-%d", i)
			validatorTemplateDefinition.Description = fmt.Sprintf("validator-%d description", i)
			validatorTemplateDefinition.Website = fmt.Sprintf("https://validator-%d", i)

			// Assign validatorDefinitions and allocations at fixed indices to avoid data races across the error group's goroutines.
			validatorDefinitions[i] = validatorTemplateDefinition

			// self delegation
			allocations[2*i] = GenesisAppStateAllocation{
				Amount:  100_000_000_000,
				Denom:   fmt.Sprintf("udelegation_%s", validatorTemplateDefinition.IdentityKey),
				Address: validatorTemplateDefinition.FundingStreams[0].Address,
			}
			// liquid
			allocations[2*i+1] = GenesisAppStateAllocation{
				Amount:  1_000_000_000_000,
				Denom:   chainCfg.Denom,
				Address: validatorTemplateDefinition.FundingStreams[0].Address,
			}

			return nil
		})
	}

	for _, wallet := range additionalGenesisWallets {
		allocations = append(allocations, GenesisAppStateAllocation{
			Address: wallet.Address,
			Denom:   wallet.Denom,
			Amount:  wallet.Amount,
		})
	}

	for _, n := range fullnodes {
		n := n
		eg.Go(func() error { return n.TendermintNode.InitFullNodeFiles(egCtx) })
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("waiting to init full nodes' files: %w", err)
	}

	firstVal := c.PenumbraNodes[0]
	if err := firstVal.AppNode.GenerateGenesisFile(ctx, chainCfg.ChainID, validatorDefinitions, allocations); err != nil {
		return fmt.Errorf("generating genesis file: %w", err)
	}

	// penumbra generate-testnet right now overwrites new validator keys
	eg, egCtx = errgroup.WithContext(ctx)
	for i, val := range c.PenumbraNodes[:c.numValidators] {
		i := i
		val := val
		// Use an errgroup to save some time doing many concurrent copies inside containers.
		eg.Go(func() error {
			firstValPrivKeyRelPath := fmt.Sprintf(".penumbra/testnet_data/node%d/tendermint/config/priv_validator_key.json", i)

			fr := dockerutil.NewFileRetriever(c.log, firstVal.AppNode.DockerClient, firstVal.AppNode.TestName)
			pk, err := fr.SingleFileContent(egCtx, firstVal.AppNode.VolumeName, firstValPrivKeyRelPath)
			if err != nil {
				return fmt.Errorf("error getting validator private key content: %w", err)
			}

			fw := dockerutil.NewFileWriter(c.log, val.AppNode.DockerClient, val.AppNode.TestName)
			if err := fw.WriteFile(egCtx, val.TendermintNode.VolumeName, "config/priv_validator_key.json", pk); err != nil {
				return fmt.Errorf("overwriting priv_validator_key.json: %w", err)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return c.start(ctx)
}

// Bootstraps the chain and starts it from genesis
func (c *Chain) start(ctx context.Context) error {
	// Copy the penumbra genesis to all tendermint nodes.
	genesisContent, err := c.PenumbraNodes[0].AppNode.genesisFileContent(ctx)
	if err != nil {
		return err
	}

	tendermintNodes := make([]*tendermint.Node, len(c.PenumbraNodes))
	for i, node := range c.PenumbraNodes {
		tendermintNodes[i] = node.TendermintNode
		if err := node.TendermintNode.OverwriteGenesisFile(ctx, genesisContent); err != nil {
			return err
		}
	}

	tmNodes := tendermint.Nodes(tendermintNodes)

	if err := tmNodes.LogGenesisHashes(ctx); err != nil {
		return err
	}

	eg, egCtx := errgroup.WithContext(ctx)
	for _, n := range c.PenumbraNodes {
		n := n
		sep, err := n.TendermintNode.GetConfigSeparator()
		if err != nil {
			return err
		}
		eg.Go(func() error {
			return n.TendermintNode.CreateNodeContainer(
				egCtx,
				fmt.Sprintf("--proxy%sapp=tcp://%s:26658", sep, n.AppNode.HostName()),
				"--rpc.laddr=tcp://0.0.0.0:26657",
			)
		})
		eg.Go(func() error {
			return n.AppNode.CreateNodeContainer(egCtx)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	eg, egCtx = errgroup.WithContext(ctx)
	for _, n := range c.PenumbraNodes {
		n := n
		c.log.Info("Starting tendermint container", zap.String("container", n.TendermintNode.Name()))
		eg.Go(func() error {
			peers := tmNodes.PeerString(egCtx, n.TendermintNode)
			if err := n.TendermintNode.SetConfigAndPeers(egCtx, peers); err != nil {
				return err
			}
			return n.TendermintNode.StartContainer(egCtx)
		})
		c.log.Info("Starting penumbra container", zap.String("container", n.AppNode.Name()))
		eg.Go(func() error {
			return n.AppNode.StartContainer(egCtx)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	// Wait for 5 blocks before considering the chains "started"
	return testutil.WaitForBlocks(ctx, 5, c.getRelayerNode().TendermintNode)
}
