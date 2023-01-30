package polkadot

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	gsrpc "github.com/centrifuge/go-substrate-rpc-client/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/icza/dyno"
	p2pcrypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/strangelove-ventures/interchaintest/v6/ibc"
	"github.com/strangelove-ventures/interchaintest/v6/internal/dockerutil"
	"go.uber.org/zap"
)

// ParachainNode defines the properties required for running a polkadot parachain node.
type ParachainNode struct {
	log      *zap.Logger
	TestName string
	Index    int

	NetworkID    string
	containerID  string
	VolumeName   string
	DockerClient *client.Client
	Image        ibc.DockerImage

	Chain           ibc.Chain
	Bin             string
	NodeKey         p2pcrypto.PrivKey
	ChainID         string
	Flags           []string
	RelayChainFlags []string

	api         *gsrpc.SubstrateAPI
	hostWsPort  string
	hostRpcPort string
}

type ParachainNodes []*ParachainNode

// Name returns the name of the test node container.
func (pn *ParachainNode) Name() string {
	return fmt.Sprintf("%s-%d-%s-%s", pn.Bin, pn.Index, pn.ChainID, dockerutil.SanitizeContainerName(pn.TestName))
}

// HostName returns the docker hostname of the test container.
func (pn *ParachainNode) HostName() string {
	return dockerutil.CondenseHostName(pn.Name())
}

// Bind returns the home folder bind point for running the node.
func (pn *ParachainNode) Bind() []string {
	return []string{fmt.Sprintf("%s:%s", pn.VolumeName, pn.NodeHome())}
}

// NodeHome returns the working directory within the docker image,
// the path where the docker volume is mounted.
func (pn *ParachainNode) NodeHome() string {
	return "/home/heighliner"
}

// ParachainChainSpecFileName returns the relative path to the chain spec file
// within the parachain container.
func (pn *ParachainNode) ParachainChainSpecFileName() string {
	return fmt.Sprintf("%s.json", pn.ChainID)
}

// ParachainChainSpecFilePathFull returns the full path to the chain spec file
// within the parachain container
func (pn *ParachainNode) ParachainChainSpecFilePathFull() string {
	return filepath.Join(pn.NodeHome(), pn.ParachainChainSpecFileName())
}

// RawRelayChainSpecFilePathFull returns the full path to the raw relay chain spec file
// within the container.
func (pn *ParachainNode) RawRelayChainSpecFilePathFull() string {
	return filepath.Join(pn.NodeHome(), fmt.Sprintf("%s-raw.json", pn.Chain.Config().ChainID))
}

// RawRelayChainSpecFilePathRelative returns the relative path to the raw relay chain spec file
// within the container.
func (pn *ParachainNode) RawRelayChainSpecFilePathRelative() string {
	return fmt.Sprintf("%s-raw.json", pn.Chain.Config().ChainID)
}

// PeerID returns the public key of the node key for p2p.
func (pn *ParachainNode) PeerID() (string, error) {
	id, err := peer.IDFromPrivateKey(pn.NodeKey)
	if err != nil {
		return "", err
	}
	return peer.Encode(id), nil
}

// MultiAddress returns the p2p multiaddr of the node.
func (pn *ParachainNode) MultiAddress() (string, error) {
	peerId, err := pn.PeerID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/dns4/%s/tcp/%s/p2p/%s", pn.HostName(), strings.Split(nodePort, "/")[0], peerId), nil
}

type GetParachainIDResponse struct {
	ParachainID int `json:"para_id"`
}

// GenerateDefaultChainSpec runs build-spec to get the default chain spec into something malleable
func (pn *ParachainNode) GenerateDefaultChainSpec(ctx context.Context) ([]byte, error) {
	cmd := []string{
		pn.Bin,
		"build-spec",
		fmt.Sprintf("--chain=%s", pn.ChainID),
	}
	res := pn.Exec(ctx, cmd, nil)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Stdout, nil
}

// GenerateParachainGenesisFile creates the default chain spec, modifies it and returns it.
// The modified chain spec is then written to each Parachain node
func (pn *ParachainNode) GenerateParachainGenesisFile(ctx context.Context, additionalGenesisWallets ...ibc.WalletAmount) ([]byte, error) {
	defaultChainSpec, err := pn.GenerateDefaultChainSpec(ctx)
	if err != nil {
		return nil, fmt.Errorf("error generating default parachain chain spec: %w", err)
	}

	var chainSpec interface{}
	err = json.Unmarshal(defaultChainSpec, &chainSpec)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling parachain chain spec: %w", err)
	}

	balances, err := dyno.GetSlice(chainSpec, "genesis", "runtime", "balances", "balances")
	if err != nil {
		return nil, fmt.Errorf("error getting balances from parachain chain spec: %w", err)
	}

	for _, wallet := range additionalGenesisWallets {
		balances = append(balances,
			[]interface{}{wallet.Address, wallet.Amount},
		)
	}
	if err := dyno.Set(chainSpec, balances, "genesis", "runtime", "balances", "balances"); err != nil {
		return nil, fmt.Errorf("error setting parachain balances: %w", err)
	}
	editedChainSpec, err := json.MarshalIndent(chainSpec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("error marshaling modified parachain chain spec: %w", err)
	}

	return editedChainSpec, nil
}

// ParachainID retrieves the node parachain ID.
func (pn *ParachainNode) ParachainID(ctx context.Context) (int, error) {
	cmd := []string{
		pn.Bin,
		"build-spec",
		fmt.Sprintf("--chain=%s", pn.ChainID),
	}
	res := pn.Exec(ctx, cmd, nil)
	if res.Err != nil {
		return -1, res.Err
	}
	out := GetParachainIDResponse{}
	if err := json.Unmarshal([]byte(res.Stdout), &out); err != nil {
		return -1, err
	}
	return out.ParachainID, nil
}

// ExportGenesisWasm exports the genesis wasm json for the configured chain ID.
func (pn *ParachainNode) ExportGenesisWasm(ctx context.Context) (string, error) {
	cmd := []string{
		pn.Bin,
		"export-genesis-wasm",
		fmt.Sprintf("--chain=%s", pn.ParachainChainSpecFilePathFull()),
	}
	res := pn.Exec(ctx, cmd, nil)
	if res.Err != nil {
		return "", res.Err
	}
	return string(res.Stdout), nil
}

// ExportGenesisState exports the genesis state json for the configured chain ID.
func (pn *ParachainNode) ExportGenesisState(ctx context.Context) (string, error) {
	cmd := []string{
		pn.Bin,
		"export-genesis-state",
		fmt.Sprintf("--chain=%s", pn.ParachainChainSpecFilePathFull()),
	}
	res := pn.Exec(ctx, cmd, nil)
	if res.Err != nil {
		return "", res.Err
	}
	return string(res.Stdout), nil
}

func (pn *ParachainNode) logger() *zap.Logger {
	return pn.log.With(
		zap.String("chain_id", pn.ChainID),
		zap.String("test", pn.TestName),
	)
}

// CreateNodeContainer assembles a parachain node docker container ready to launch.
func (pn *ParachainNode) CreateNodeContainer(ctx context.Context) error {
	nodeKey, err := pn.NodeKey.Raw()
	if err != nil {
		return fmt.Errorf("error getting ed25519 node key: %w", err)
	}
	multiAddress, err := pn.MultiAddress()
	if err != nil {
		return err
	}
	cmd := []string{
		pn.Bin,
		fmt.Sprintf("--ws-port=%s", strings.Split(wsPort, "/")[0]),
		"--collator",
		fmt.Sprintf("--node-key=%s", hex.EncodeToString(nodeKey[0:32])),
		fmt.Sprintf("--%s", IndexedName[pn.Index]),
		"--unsafe-ws-external",
		"--unsafe-rpc-external",
		"--prometheus-external",
		"--rpc-cors=all",
		fmt.Sprintf("--prometheus-port=%s", strings.Split(prometheusPort, "/")[0]),
		fmt.Sprintf("--listen-addr=/ip4/0.0.0.0/tcp/%s", strings.Split(nodePort, "/")[0]),
		fmt.Sprintf("--public-addr=%s", multiAddress),
		"--base-path", pn.NodeHome(),
		fmt.Sprintf("--chain=%s", pn.ParachainChainSpecFilePathFull()),
	}
	cmd = append(cmd, pn.Flags...)
	cmd = append(cmd, "--", fmt.Sprintf("--chain=%s", pn.RawRelayChainSpecFilePathFull()))
	cmd = append(cmd, pn.RelayChainFlags...)
	pn.logger().
		Info("Running command",
			zap.String("command", strings.Join(cmd, " ")),
			zap.String("container", pn.Name()),
		)

	cc, err := pn.DockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image: pn.Image.Ref(),

			Entrypoint: []string{},
			Cmd:        cmd,

			Hostname: pn.HostName(),
			User:     pn.Image.UidGid,

			Labels: map[string]string{dockerutil.CleanupLabel: pn.TestName},

			ExposedPorts: exposedPorts,
		},
		&container.HostConfig{
			Binds:           pn.Bind(),
			PublishAllPorts: true,
			AutoRemove:      false,
			DNS:             []string{},
		},
		&network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				pn.NetworkID: {},
			},
		},
		nil,
		pn.Name(),
	)
	if err != nil {
		return err
	}
	pn.containerID = cc.ID
	return nil
}

// StopContainer stops the relay chain node container, waiting at most 30 seconds.
func (pn *ParachainNode) StopContainer(ctx context.Context) error {
	timeout := 30 * time.Second
	return pn.DockerClient.ContainerStop(ctx, pn.containerID, &timeout)
}

// StartContainer starts the container after it is built by CreateNodeContainer.
func (pn *ParachainNode) StartContainer(ctx context.Context) error {
	if err := dockerutil.StartContainer(ctx, pn.DockerClient, pn.containerID); err != nil {
		return err
	}

	c, err := pn.DockerClient.ContainerInspect(ctx, pn.containerID)
	if err != nil {
		return err
	}

	// Set the host ports once since they will not change after the container has started.
	pn.hostWsPort = dockerutil.GetHostPort(c, wsPort)
	pn.hostRpcPort = dockerutil.GetHostPort(c, rpcPort)

	explorerUrl := fmt.Sprintf("\033[4;34mhttps://polkadot.js.org/apps?rpc=ws://%s#/explorer\033[0m",
		strings.Replace(pn.hostWsPort, "localhost", "127.0.0.1", 1))
	pn.log.Info(explorerUrl, zap.String("container", pn.Name()))
	var api *gsrpc.SubstrateAPI
	if err = retry.Do(func() error {
		var err error
		api, err = gsrpc.NewSubstrateAPI("ws://" + pn.hostWsPort)
		return err
	}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr); err != nil {
		return err
	}

	pn.api = api
	return nil
}

// Exec run a container for a specific job and block until the container exits.
func (pn *ParachainNode) Exec(ctx context.Context, cmd []string, env []string) dockerutil.ContainerExecResult {
	job := dockerutil.NewImage(pn.log, pn.DockerClient, pn.NetworkID, pn.TestName, pn.Image.Repository, pn.Image.Version)
	opts := dockerutil.ContainerOptions{
		Binds: pn.Bind(),
		Env:   env,
		User:  pn.Image.UidGid,
	}
	return job.Run(ctx, cmd, opts)
}

func (pn *ParachainNode) GetBalance(ctx context.Context, address string, denom string) (int64, error) {
	return GetBalance(pn.api, address)
}

// GetIbcBalance returns the Coins type of ibc coins in account
// [Add back when we move from centrifuge -> ComposableFi's go-substrate-rpc-client (for ibc methods)]
/*func (pn *ParachainNode) GetIbcBalance(ctx context.Context, address []byte) (sdktypes.Coins, error) {
	res, err := pn.api.RPC.IBC.QueryBalanceWithAddress(ctx, address)
	if err != nil {
		return nil, err
	}
	return res, nil
}*/

// SendFunds sends funds to a wallet from a user account.
// Implements Chain interface.
func (pn *ParachainNode) SendFunds(ctx context.Context, keyName string, amount ibc.WalletAmount) error {
	kp, err := pn.Chain.(*PolkadotChain).GetKeyringPair(keyName)
	if err != nil {
		return err
	}

	hash, err := SendFundsTx(pn.api, kp, amount)
	if err != nil {
		return err
	}

	pn.log.Info("Transfer sent", zap.String("hash", fmt.Sprintf("%#x", hash)), zap.String("container", pn.Name()))
	return nil
}
