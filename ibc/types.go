package ibc

import (
	"context"
	"fmt"
	"io"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/google/go-cmp/cmp"

	"cosmossdk.io/math"

	ibcexported "github.com/cosmos/ibc-go/v8/modules/core/03-connection/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/module/testutil"
)

// Chain type constant values, used to determine if a ChainConfig is of a certain type.
const (
	Polkadot   = "polkadot"
	Parachain  = "parachain"
	RelayChain = "relaychain"
	Cosmos     = "cosmos"
	Penumbra   = "penumbra"
	Ethereum   = "ethereum"
	Thorchain  = "thorchain"
	UTXO       = "utxo"
	Namada     = "namada"
)

// ChainConfig defines the chain parameters requires to run an interchaintest testnet for a chain.
type ChainConfig struct {
	// Chain type, e.g. cosmos.
	Type string `yaml:"type"`
	// Chain name, e.g. cosmoshub.
	Name string `yaml:"name"`
	// Chain ID, e.g. cosmoshub-4
	ChainID string `yaml:"chain-id"`
	// Docker images required for running chain nodes.
	Images []DockerImage `yaml:"images"`
	// https://github.com/informalsystems/CometMock usage.
	CometMock CometMockConfig `yaml:"comet-mock-image"`
	// Binary to execute for the chain node daemon.
	Bin string `yaml:"bin"`
	// Bech32 prefix for chain addresses, e.g. cosmos.
	Bech32Prefix string `yaml:"bech32-prefix"`
	// Denomination of native currency, e.g. uatom.
	Denom string `yaml:"denom"`
	// Coin type
	CoinType string `default:"118" yaml:"coin-type"`
	// Key signature algorithm
	SigningAlgorithm string `default:"secp256k1" yaml:"signing-algorithm"`
	// Minimum gas prices for sending transactions, in native currency denom.
	GasPrices string `yaml:"gas-prices"`
	// Adjustment multiplier for gas fees.
	GasAdjustment float64 `yaml:"gas-adjustment"`
	// Default gas limit for transactions. May be empty, "auto", or a number.
	Gas string `yaml:"gas" default:"auto"`
	// Trusting period of the chain.
	TrustingPeriod string `yaml:"trusting-period"`
	// Do not use docker host mount.
	NoHostMount bool `yaml:"no-host-mount"`
	// When true, will skip validator gentx flow
	SkipGenTx bool
	// When provided, will run before performing gentx and genesis file creation steps for validators.
	PreGenesis func(Chain) error
	// When provided, genesis file contents will be altered before sharing for genesis.
	ModifyGenesis func(ChainConfig, []byte) ([]byte, error)
	// Modify genesis-amounts for the validator at the given index
	ModifyGenesisAmounts func(int) (sdk.Coin, sdk.Coin)
	// Override config parameters for files at filepath.
	ConfigFileOverrides map[string]any
	// Non-nil will override the encoding config, used for cosmos chains only.
	EncodingConfig *testutil.TestEncodingConfig
	// Required when the chain requires the chain-id field to be populated for certain commands
	UsingChainIDFlagCLI bool `yaml:"using-chain-id-flag-cli"`
	// Configuration describing additional sidecar processes.
	SidecarConfigs []SidecarConfig
	// Configuration describing additional interchain security options.
	InterchainSecurityConfig ICSConfig
	// CoinDecimals for the chains base micro/nano/atto token configuration.
	CoinDecimals *int64
	// HostPortOverride exposes ports to the host.
	// To avoid port binding conflicts, ports are only exposed on the 0th validator.
	HostPortOverride map[int]int `yaml:"host-port-override"`
	// ExposeAdditionalPorts exposes each port id to the host on a random port. ex: "8080/tcp"
	// Access the address with ChainNode.GetHostAddress
	ExposeAdditionalPorts []string
	// Additional start command arguments
	AdditionalStartArgs []string
	// Environment variables for chain nodes
	Env []string
	// Genesis file contents for the chain
	// Used if starting from an already populated genesis.json, e.g for hard fork upgrades.
	// When nil, the chain will generate the number of validators specified in the ChainSpec.
	Genesis *GenesisConfig
}

func (c ChainConfig) Clone() ChainConfig {
	x := c

	images := make([]DockerImage, len(c.Images))
	copy(images, c.Images)
	x.Images = images

	sidecars := make([]SidecarConfig, len(c.SidecarConfigs))
	copy(sidecars, c.SidecarConfigs)
	x.SidecarConfigs = sidecars

	additionalPorts := make([]string, len(c.ExposeAdditionalPorts))
	copy(additionalPorts, c.ExposeAdditionalPorts)
	x.ExposeAdditionalPorts = additionalPorts

	if c.CoinDecimals != nil {
		coinDecimals := *c.CoinDecimals
		x.CoinDecimals = &coinDecimals
	}

	if c.Genesis != nil {
		genesis := *c.Genesis
		x.Genesis = &genesis
	}

	return x
}

func (c ChainConfig) UsesCometMock() bool {
	img := c.CometMock.Image
	return img.Repository != "" && img.Version != ""
}

func (c ChainConfig) VerifyCoinType() (string, error) {
	// If coin-type is left blank in the ChainConfig,
	// the Cosmos SDK default of 118 is used.
	if c.CoinType == "" {
		typ := reflect.TypeOf(c)
		f, _ := typ.FieldByName("CoinType")
		coinType := f.Tag.Get("default")
		_, err := strconv.ParseUint(coinType, 10, 32)
		if err != nil {
			return "", err
		}
		return coinType, nil
	} else {
		_, err := strconv.ParseUint(c.CoinType, 10, 32)
		if err != nil {
			return "", err
		}
		return c.CoinType, nil
	}
}

func (c ChainConfig) MergeChainSpecConfig(other ChainConfig) ChainConfig {
	// Make several in-place modifications to c,
	// which is a value, not a reference,
	// and return the updated copy.

	if other.Type != "" {
		c.Type = other.Type
	}

	// Skip Name, as that is held in ChainSpec.ChainName.

	if other.ChainID != "" {
		c.ChainID = other.ChainID
	}

	if len(other.Images) > 0 {
		c.Images = append([]DockerImage(nil), other.Images...)
	}

	if other.UsesCometMock() {
		c.CometMock = other.CometMock
	}

	if other.Bin != "" {
		c.Bin = other.Bin
	}

	if other.Bech32Prefix != "" {
		c.Bech32Prefix = other.Bech32Prefix
	}

	if other.Denom != "" {
		c.Denom = other.Denom
	}

	if other.CoinType != "" {
		c.CoinType = other.CoinType
	}

	if other.GasPrices != "" {
		c.GasPrices = other.GasPrices
	}

	if other.GasAdjustment > 0 {
		c.GasAdjustment = other.GasAdjustment
	}

	if other.Gas != "" {
		c.Gas = other.Gas
	}

	if other.TrustingPeriod != "" {
		c.TrustingPeriod = other.TrustingPeriod
	}

	// Skip NoHostMount so that false can be distinguished.

	if other.ModifyGenesis != nil {
		c.ModifyGenesis = other.ModifyGenesis
	}

	if other.SkipGenTx {
		c.SkipGenTx = true
	}

	if other.PreGenesis != nil {
		c.PreGenesis = other.PreGenesis
	}

	if other.ConfigFileOverrides != nil {
		c.ConfigFileOverrides = other.ConfigFileOverrides
	}

	if other.EncodingConfig != nil {
		c.EncodingConfig = other.EncodingConfig
	}

	if len(other.SidecarConfigs) > 0 {
		c.SidecarConfigs = append([]SidecarConfig(nil), other.SidecarConfigs...)
	}

	if other.CoinDecimals != nil {
		c.CoinDecimals = other.CoinDecimals
	}
	if other.AdditionalStartArgs != nil {
		c.AdditionalStartArgs = append(c.AdditionalStartArgs, other.AdditionalStartArgs...)
	}

	if other.Env != nil {
		c.Env = append(c.Env, other.Env...)
	}

	if len(other.ExposeAdditionalPorts) > 0 {
		c.ExposeAdditionalPorts = append(c.ExposeAdditionalPorts, other.ExposeAdditionalPorts...)
	}

	if !cmp.Equal(other.InterchainSecurityConfig, ICSConfig{}) {
		c.InterchainSecurityConfig = other.InterchainSecurityConfig
	}

	if other.Genesis != nil {
		c.Genesis = other.Genesis
	}

	return c
}

// WithCodeCoverage enables Go Code Coverage from the chain node directory.
func (c *ChainConfig) WithCodeCoverage(override ...string) {
	c.Env = append(c.Env, fmt.Sprintf("GOCOVERDIR=%s", path.Join("/var/cosmos-chain", c.Name)))
	if len(override) > 0 {
		c.Env = append(c.Env, override[0])
	}
}

// IsFullyConfigured reports whether all required fields have been set on c.
// It is possible for some fields, such as GasAdjustment and NoHostMount,
// to be their respective zero values and for IsFullyConfigured to still report true.
func (c ChainConfig) IsFullyConfigured() bool {
	for _, image := range c.Images {
		if !image.IsFullyConfigured() {
			return false
		}
	}

	return c.Type != "" &&
		c.Name != "" &&
		c.ChainID != "" &&
		len(c.Images) > 0 &&
		c.Bin != "" &&
		c.Bech32Prefix != "" &&
		c.Denom != "" &&
		c.GasPrices != "" &&
		c.TrustingPeriod != ""
}

// SidecarConfig describes the configuration options for instantiating a new sidecar process.
type SidecarConfig struct {
	ProcessName      string
	Image            DockerImage
	HomeDir          string
	Ports            []string
	StartCmd         []string
	Env              []string
	PreStart         bool
	StartCheck       func(int) error
	ValidatorProcess bool
}

type DockerImage struct {
	Repository string `json:"repository" yaml:"repository"`
	Version    string `json:"version" yaml:"version"`
	UIDGID     string `json:"uid-gid" yaml:"uid-gid"`
}

type CometMockConfig struct {
	Image       DockerImage `yaml:"image"`
	BlockTimeMs int         `yaml:"block-time"`
}

func NewDockerImage(repository, version, uidGID string) DockerImage {
	return DockerImage{
		Repository: repository,
		Version:    version,
		UIDGID:     uidGID,
	}
}

// IsFullyConfigured reports whether all of i's required fields are present.
// Version is not required, as it can be superseded by a ChainSpec version.
func (i DockerImage) IsFullyConfigured() bool {
	return i.Validate() == nil
}

// Validate returns an error describing which of i's required fields are missing
// and returns nil if all required fields are present. Version is not required,
// as it can be superseded by a ChainSpec version.
func (i DockerImage) Validate() error {
	var missing []string

	if i.Repository == "" {
		missing = append(missing, "Repository")
	}
	if i.UIDGID == "" {
		missing = append(missing, "UidGid")
	}

	if len(missing) > 0 {
		fields := strings.Join(missing, ", ")
		return fmt.Errorf("DockerImage is missing fields: %s", fields)
	}

	return nil
}

// Ref returns the reference to use when e.g. creating a container.
func (i DockerImage) Ref() string {
	if i.Version == "" {
		return i.Repository + ":latest"
	}

	return i.Repository + ":" + i.Version
}

func (i DockerImage) PullImage(ctx context.Context, client *client.Client) error {
	ref := i.Ref()
	_, _, err := client.ImageInspectWithRaw(ctx, ref)
	if err != nil {
		rc, err := client.ImagePull(ctx, ref, types.ImagePullOptions{})
		if err != nil {
			return fmt.Errorf("pull image %s: %w", ref, err)
		}
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
	}
	return nil
}

type WalletAmount struct {
	Address string
	Denom   string
	Amount  math.Int
}

type IBCTimeout struct {
	NanoSeconds uint64
	Height      int64
}

type ChannelCounterparty struct {
	PortID    string `json:"port_id"`
	ChannelID string `json:"channel_id"`
}

type ChannelOutput struct {
	State          string              `json:"state"`
	Ordering       string              `json:"ordering"`
	Counterparty   ChannelCounterparty `json:"counterparty"`
	ConnectionHops []string            `json:"connection_hops"`
	Version        string              `json:"version"`
	PortID         string              `json:"port_id"`
	ChannelID      string              `json:"channel_id"`
}

// ConnectionOutput represents the IBC connection information queried from a chain's state for a particular connection.
type ConnectionOutput struct {
	ID           string                    `json:"id,omitempty" yaml:"id"`
	ClientID     string                    `json:"client_id,omitempty" yaml:"client_id"`
	Versions     []*ibcexported.Version    `json:"versions,omitempty" yaml:"versions"`
	State        string                    `json:"state,omitempty" yaml:"state"`
	Counterparty *ibcexported.Counterparty `json:"counterparty" yaml:"counterparty"`
	DelayPeriod  string                    `json:"delay_period,omitempty" yaml:"delay_period"`
}

type ConnectionOutputs []*ConnectionOutput

type ClientOutput struct {
	ClientID    string      `json:"client_id"`
	ClientState ClientState `json:"client_state"`
}

type ClientState struct {
	ChainID string `json:"chain_id"`
}

type ClientOutputs []*ClientOutput

type Wallet interface {
	KeyName() string
	FormattedAddress() string
	Mnemonic() string
	Address() []byte
}

type RelayerImplementation int64

const (
	CosmosRly RelayerImplementation = iota
	Hermes
	Hyperspace
)

// ChannelFilter provides the means for either creating an allowlist or a denylist of channels on the src chain
// which will be used to narrow down the list of channels a user wants to relay on.
type ChannelFilter struct {
	Rule        string
	ChannelList []string
}

type PathUpdateOptions struct {
	ChannelFilter *ChannelFilter
	SrcClientID   *string
	SrcConnID     *string
	SrcChainID    *string
	DstClientID   *string
	DstConnID     *string
	DstChainID    *string
}

type ICSConfig struct {
	ProviderVerOverride     string         `yaml:"provider,omitempty" json:"provider,omitempty"`
	ConsumerVerOverride     string         `yaml:"consumer,omitempty" json:"consumer,omitempty"`
	ConsumerCopyProviderKey func(int) bool `yaml:"-" json:"-"`
}

// GenesisConfig is used to start a chain from a pre-defined genesis state.
type GenesisConfig struct {
	// Genesis file contents for the chain (e.g. genesis.json for CometBFT chains).
	Contents []byte

	// If true, all validators will be emulated in the genesis file.
	// By default, only the first 2/3 (sorted by Voting Power desc) of validators will be emulated.
	AllValidators bool

	// MaxVals is a safeguard so that we don't accidentally emulate too many validators. Defaults to 10.
	// If more than MaxVals validators are required to meet 2/3 VP, the test will fail.
	MaxVals int
}
