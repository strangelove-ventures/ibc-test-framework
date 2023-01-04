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
	gstypes "github.com/centrifuge/go-substrate-rpc-client/v4/types"
	signature "github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"

	p2pCrypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/zap"

	"github.com/decred/dcrd/dcrec/secp256k1/v2"
	"github.com/strangelove-ventures/ibctest/v6/ibc"
	"github.com/strangelove-ventures/ibctest/v6/internal/dockerutil"
)

// RelayChainNode defines the properties required for running a polkadot relay chain node.
type RelayChainNode struct {
	log      *zap.Logger
	TestName string
	Index    int

	NetworkID    string
	containerID  string
	VolumeName   string
	DockerClient *client.Client
	Image        ibc.DockerImage

	Chain             ibc.Chain
	NodeKey           p2pCrypto.PrivKey
	AccountKeyName    string
	StashKeyName      string
	Ed25519PrivateKey p2pCrypto.PrivKey
	EcdsaPrivateKey   secp256k1.PrivateKey

	api         *gsrpc.SubstrateAPI
	hostWsPort  string
	hostRpcPort string
}

type RelayChainNodes []*RelayChainNode

const (
	wsPort         = "27451/tcp"
	//rpcPort        = "27452/tcp"
	nodePort       = "27452/tcp"
	rpcPort        = "9933/tcp"
	prometheusPort = "27453/tcp"
)

var (
	RtyAtt = retry.Attempts(10)
	RtyDel = retry.Delay(time.Second * 2)
	RtyErr = retry.LastErrorOnly(true)
)

var exposedPorts = map[nat.Port]struct{}{
	nat.Port(wsPort):         {},
	nat.Port(rpcPort):        {},
	nat.Port(prometheusPort): {},
	nat.Port(nodePort):       {},
}

// Name returns the name of the test node.
func (p *RelayChainNode) Name() string {
	return fmt.Sprintf("relaychain-%d-%s-%s", p.Index, p.Chain.Config().ChainID, dockerutil.SanitizeContainerName(p.TestName))
}

// HostName returns the docker hostname of the test container.
func (p *RelayChainNode) HostName() string {
	return dockerutil.CondenseHostName(p.Name())
}

// Bind returns the home folder bind point for running the node.
func (p *RelayChainNode) Bind() []string {
	return []string{fmt.Sprintf("%s:%s", p.VolumeName, p.NodeHome())}
}

// NodeHome returns the working directory within the docker image,
// the path where the docker volume is mounted.
func (p *RelayChainNode) NodeHome() string {
	return "/home/heighliner"
}

// PeerID returns the public key of the node key for p2p.
func (p *RelayChainNode) PeerID() (string, error) {
	id, err := peer.IDFromPrivateKey(p.NodeKey)
	if err != nil {
		return "", err
	}
	return peer.Encode(id), nil
}

// GrandpaAddress returns the ss58 encoded grandpa (consensus) address.
func (p *RelayChainNode) GrandpaAddress() (string, error) {
	pubKey, err := p.Ed25519PrivateKey.GetPublic().Raw()
	if err != nil {
		return "", fmt.Errorf("error fetching pubkey bytes: %w", err)
	}
	return EncodeAddressSS58(pubKey)
}

// EcdsaAddress returns the ss58 encoded secp256k1 address.
func (p *RelayChainNode) EcdsaAddress() (string, error) {
	pubKey := []byte{}
	y := p.EcdsaPrivateKey.PublicKey.Y.Bytes()
	if y[len(y)-1]%2 == 0 {
		pubKey = append(pubKey, 0x02)
	} else {
		pubKey = append(pubKey, 0x03)
	}
	pubKey = append(pubKey, p.EcdsaPrivateKey.PublicKey.X.Bytes()...)
	return EncodeAddressSS58(pubKey)
}

// MultiAddress returns the p2p multiaddr of the node.
func (p *RelayChainNode) MultiAddress() (string, error) {
	peerId, err := p.PeerID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/dns4/%s/tcp/%s/p2p/%s", p.HostName(), strings.Split(nodePort, "/")[0], peerId), nil
}

func (c *RelayChainNode) logger() *zap.Logger {
	return c.log.With(
		zap.String("chain_id", c.Chain.Config().ChainID),
		zap.String("test", c.TestName),
	)
}

// ChainSpecFilePathContainer returns the relative path to the chain spec file
// within the container.
func (p *RelayChainNode) ChainSpecFilePathContainer() string {
	return fmt.Sprintf("%s.json", p.Chain.Config().ChainID)
}

// RawChainSpecFilePathFull returns the full path to the raw chain spec file
// within the container.
func (p *RelayChainNode) RawChainSpecFilePathFull() string {
	return filepath.Join(p.NodeHome(), fmt.Sprintf("%s-raw.json", p.Chain.Config().ChainID))
}

// RawChainSpecFilePathRelative returns the relative path to the raw chain spec file
// within the container.
func (p *RelayChainNode) RawChainSpecFilePathRelative() string {
	return fmt.Sprintf("%s-raw.json", p.Chain.Config().ChainID)
}

// GenerateChainSpec builds the chain spec for the configured chain ID.
func (p *RelayChainNode) GenerateChainSpec(ctx context.Context) error {
	chainCfg := p.Chain.Config()
	cmd := []string{
		chainCfg.Bin,
		"build-spec",
		fmt.Sprintf("--chain=%s", chainCfg.ChainID),
		"--disable-default-bootnode",
	}
	res := p.Exec(ctx, cmd, nil)
	if res.Err != nil {
		return res.Err
	}
	fw := dockerutil.NewFileWriter(p.logger(), p.DockerClient, p.TestName)
	return fw.WriteFile(ctx, p.VolumeName, p.ChainSpecFilePathContainer(), res.Stdout)
}

// GenerateChainSpecRaw builds the raw chain spec from the generated chain spec
// for the configured chain ID.
func (p *RelayChainNode) GenerateChainSpecRaw(ctx context.Context) error {
	chainCfg := p.Chain.Config()
	cmd := []string{
		chainCfg.Bin,
		"build-spec",
		fmt.Sprintf("--chain=%s.json", filepath.Join(p.NodeHome(), chainCfg.ChainID)),
		"--raw",
	}
	res := p.Exec(ctx, cmd, nil)
	if res.Err != nil {
		return res.Err
	}
	fw := dockerutil.NewFileWriter(p.logger(), p.DockerClient, p.TestName)
	return fw.WriteFile(ctx, p.VolumeName, p.RawChainSpecFilePathRelative(), res.Stdout)
}

// CreateNodeContainer assembles a relay chain node docker container ready to launch.
func (p *RelayChainNode) CreateNodeContainer(ctx context.Context) error {
	nodeKey, err := p.NodeKey.Raw()
	if err != nil {
		return fmt.Errorf("error getting ed25519 node key: %w", err)
	}
	multiAddress, err := p.MultiAddress()
	if err != nil {
		return err
	}
	chainCfg := p.Chain.Config()
	cmd := []string{
		chainCfg.Bin,
		fmt.Sprintf("--chain=%s", p.RawChainSpecFilePathFull()),
		fmt.Sprintf("--ws-port=%s", strings.Split(wsPort, "/")[0]),
		fmt.Sprintf("--%s", IndexedName[p.Index]),
		fmt.Sprintf("--node-key=%s", hex.EncodeToString(nodeKey[0:32])),
		// "--validator",
		"--ws-external",
		"--rpc-external",
		"--beefy",
		"--rpc-cors=all",
		"--unsafe-ws-external",
		"--unsafe-rpc-external",
		"--prometheus-external",
		fmt.Sprintf("--prometheus-port=%s", strings.Split(prometheusPort, "/")[0]),
		fmt.Sprintf("--listen-addr=/ip4/0.0.0.0/tcp/%s", strings.Split(nodePort, "/")[0]),
		fmt.Sprintf("--public-addr=%s", multiAddress),
		"--base-path", p.NodeHome(),
	}
	p.logger().
		Info("Running command",
			zap.String("command", strings.Join(cmd, " ")),
			zap.String("container", p.Name()),
		)

	cc, err := p.DockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image: p.Image.Ref(),

			Entrypoint: []string{},
			Cmd:        cmd,

			Hostname: p.HostName(),
			User:     p.Image.UidGid,

			Labels: map[string]string{dockerutil.CleanupLabel: p.TestName},

			ExposedPorts: exposedPorts,
		},
		&container.HostConfig{
			Binds:           p.Bind(),
			PublishAllPorts: true,
			AutoRemove:      false,
			DNS:             []string{},
		},
		&network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				p.NetworkID: {},
			},
		},
		nil,
		p.Name(),
	)
	if err != nil {
		return err
	}
	p.containerID = cc.ID
	return nil
}

// StopContainer stops the relay chain node container, waiting at most 30 seconds.
func (p *RelayChainNode) StopContainer(ctx context.Context) error {
	timeout := 30 * time.Second
	return p.DockerClient.ContainerStop(ctx, p.containerID, &timeout)
}

// StartContainer starts the container after it is built by CreateNodeContainer.
func (p *RelayChainNode) StartContainer(ctx context.Context) error {
	if err := dockerutil.StartContainer(ctx, p.DockerClient, p.containerID); err != nil {
		return err
	}

	c, err := p.DockerClient.ContainerInspect(ctx, p.containerID)
	if err != nil {
		return err
	}

	// Set the host ports once since they will not change after the container has started.
	p.hostWsPort = dockerutil.GetHostPort(c, wsPort)
	p.hostRpcPort = dockerutil.GetHostPort(c, rpcPort)

	p.logger().Info("Waiting for RPC endpoint to be available", zap.String("container", p.Name()))
	fmt.Printf("\033[4;34mhttps://polkadot.js.org/apps?rpc=ws://%s#/explorer\033[0m    %s\n",
		strings.Replace(p.hostWsPort, "localhost", "127.0.0.1", 1), p.Name())
	var api *gsrpc.SubstrateAPI
	if err = retry.Do(func() error {
		var err error
		api, err = gsrpc.NewSubstrateAPI("ws://" + p.hostWsPort)
		return err
	}, retry.Context(ctx), RtyAtt, RtyDel, RtyErr); err != nil {
		return err
	}

	p.logger().Info("Done", zap.String("container", p.Name()))
	p.api = api

	return nil
}

// Exec runs a container for a specific job and blocks until the container exits.
func (p *RelayChainNode) Exec(ctx context.Context, cmd []string, env []string) dockerutil.ContainerExecResult {
	job := dockerutil.NewImage(p.log, p.DockerClient, p.NetworkID, p.TestName, p.Image.Repository, p.Image.Version)
	opts := dockerutil.ContainerOptions{
		Binds: p.Bind(),
		Env:   env,
		User:  p.Image.UidGid,
	}
	return job.Run(ctx, cmd, opts)
}

// SendFunds sends funds to a wallet from a user account.
// Implements Chain interface.
func (p *RelayChainNode) SendFunds(ctx context.Context, keyName string, amount ibc.WalletAmount) error {
	meta, err := p.api.RPC.State.GetMetadataLatest()
	if err != nil {
		return err
	}

	receiverPubKey, err := DecodeAddressSS58(amount.Address)
	if err != nil {
		return err
	}

	receiver, err := gstypes.NewMultiAddressFromHexAccountID(hex.EncodeToString(receiverPubKey))
	if err != nil {
		return err
	}

	call, err := gstypes.NewCall(meta, "Balances.transfer", receiver, gstypes.NewUCompactFromUInt(uint64(amount.Amount)))
	if err != nil {
		return err
	}	

	// Create the extrinsic
	ext := gstypes.NewExtrinsic(call)
	genesisHash, err := p.api.RPC.Chain.GetBlockHash(0)
	if err != nil {
		return err
	}

	rv, err := p.api.RPC.State.GetRuntimeVersionLatest()
	if err != nil {
		return err
	}

	krItem, err := p.Chain.(*PolkadotChain).Keyring().Get(keyName)
	if err != nil {
		return err
	}

	kp := signature.KeyringPair{}
	err = json.Unmarshal(krItem.Data, &kp)
	if err != nil {
		return err
	}

	pubKey, err := DecodeAddressSS58(kp.Address)
	if err != nil {
		return err
	}

	key, err := gstypes.CreateStorageKey(meta, "System", "Account", pubKey)
	if err != nil {
		return err
	}

	var accountInfo AccountInfo
	ok, err := p.api.RPC.State.GetStorageLatest(key, &accountInfo)
	if err != nil || !ok {
		return err
	}

	nonce := uint32(accountInfo.Nonce)
	o := gstypes.SignatureOptions{
		BlockHash:   genesisHash,
		Era:         gstypes.ExtrinsicEra{IsMortalEra: false},
		GenesisHash: genesisHash,
		Nonce:       gstypes.NewUCompactFromUInt(uint64(nonce)),
		SpecVersion: rv.SpecVersion,
		Tip:         gstypes.NewUCompactFromUInt(0),
		TransactionVersion: rv.TransactionVersion,
	}

	// Sign the transaction using Alice's default account
	err = ext.Sign(kp, o)
	if err != nil {
		return err
	}

	// Send the extrinsic
	hash, err := p.api.RPC.Author.SubmitExtrinsic(ext)
	if err != nil {
		fmt.Printf("Panic after submitExtrinsic, hash: %#x\n", hash)
		return err
	}

	fmt.Printf("Transfer sent with hash %#x\n", hash)
	return nil
}

// GetBalance fetches the current balance for a specific account address and denom.
// Implements Chain interface.
func (p *RelayChainNode) GetBalance(ctx context.Context, address string, denom string) (int64, error) {
	meta, err := p.api.RPC.State.GetMetadataLatest()
	if err != nil {
		return -1, err
	}
	pubKey, err := DecodeAddressSS58(address)
	if err != nil {
		return -2, err
	}
	key, err := gstypes.CreateStorageKey(meta, "System", "Account", pubKey, nil)
	if err != nil {
		return -3, err
	}

	var accountInfo AccountInfo
	ok, err := p.api.RPC.State.GetStorageLatest(key, &accountInfo)
	if err != nil {
		return -4, err
	}
	if !ok {
		return -5, nil
	}

	return accountInfo.Data.Free.Int64(), nil
}