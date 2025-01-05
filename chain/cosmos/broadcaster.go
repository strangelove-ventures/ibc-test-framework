package cosmos

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/client/tx"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"

	"github.com/strangelove-ventures/interchaintest/v8/dockerutil"
	"github.com/strangelove-ventures/interchaintest/v8/testutil"
)

type ClientContextOpt func(clientContext client.Context) client.Context

type FactoryOpt func(factory tx.Factory) tx.Factory

type User interface {
	KeyName() string
	FormattedAddress() string
}

type Broadcaster struct {
	// buf stores the output sdk.TxResponse when broadcast.Tx is invoked.
	buf *bytes.Buffer
	// keyrings is a mapping of keyrings which point to a temporary test directory. The contents
	// of this directory are copied from the node container for the specific user.
	keyrings map[User]keyring.Keyring

	// chain is a reference to the CosmosChain instance which will be the target of the messages.
	chain *CosmosChain
	// t is the testing.T for the current test.
	t *testing.T

	// factoryOptions is a slice of broadcast.FactoryOpt which enables arbitrary configuration of the tx.Factory.
	factoryOptions []FactoryOpt
	// clientContextOptions is a slice of broadcast.ClientContextOpt which enables arbitrary configuration of the client.Context.
	clientContextOptions []ClientContextOpt
}

// NewBroadcaster returns an instance of Broadcaster which can be used with broadcast.Tx to
// broadcast messages sdk messages.
func NewBroadcaster(t *testing.T, chain *CosmosChain) *Broadcaster {
	t.Helper()

	return &Broadcaster{
		t:        t,
		chain:    chain,
		buf:      &bytes.Buffer{},
		keyrings: map[User]keyring.Keyring{},
	}
}

// ConfigureFactoryOptions ensure the given configuration functions are run when calling GetFactory
// after all default options have been applied.
func (b *Broadcaster) ConfigureFactoryOptions(opts ...FactoryOpt) {
	b.factoryOptions = append(b.factoryOptions, opts...)
}

// ConfigureClientContextOptions ensure the given configuration functions are run when calling GetClientContext
// after all default options have been applied.
func (b *Broadcaster) ConfigureClientContextOptions(opts ...ClientContextOpt) {
	b.clientContextOptions = append(b.clientContextOptions, opts...)
}

// GetFactory returns an instance of tx.Factory that is configured with this Broadcaster's CosmosChain
// and the provided user. ConfigureFactoryOptions can be used to specify arbitrary options to configure the returned
// factory.
func (b *Broadcaster) GetFactory(ctx context.Context, user User) (tx.Factory, error) {
	clientContext, err := b.GetClientContext(ctx, user)
	if err != nil {
		return tx.Factory{}, err
	}

	sdkAdd, err := b.chain.AccAddressFromBech32(user.FormattedAddress())
	if err != nil {
		return tx.Factory{}, err
	}

	account, err := clientContext.AccountRetriever.GetAccount(clientContext, sdkAdd)
	if err != nil {
		return tx.Factory{}, err
	}

	f := b.defaultTxFactory(clientContext, account)
	for _, opt := range b.factoryOptions {
		f = opt(f)
	}
	return f, nil
}

// GetClientContext returns a client context that is configured with this Broadcaster's CosmosChain and
// the provided user. ConfigureClientContextOptions can be used to configure arbitrary options to configure the returned
// client.Context.
func (b *Broadcaster) GetClientContext(ctx context.Context, user User) (client.Context, error) {
	chain := b.chain
	cn := chain.GetFullNode()

	_, ok := b.keyrings[user]
	if !ok {
		localDir := b.t.TempDir()
		containerKeyringDir := path.Join(cn.HomeDir(), "keyring-test")
		kr, err := dockerutil.NewLocalKeyringFromDockerContainer(ctx, cn.DockerClient, localDir, containerKeyringDir, cn.containerLifecycle.ContainerID())
		if err != nil {
			return client.Context{}, err
		}
		b.keyrings[user] = kr
	}

	sdkAdd, err := b.chain.AccAddressFromBech32(user.FormattedAddress())
	if err != nil {
		return client.Context{}, err
	}

	clientContext := b.defaultClientContext(user, sdkAdd)
	for _, opt := range b.clientContextOptions {
		clientContext = opt(clientContext)
	}
	return clientContext, nil
}

// GetTxResponseBytes returns the sdk.TxResponse bytes which returned from broadcast.Tx.
func (b *Broadcaster) GetTxResponseBytes(ctx context.Context, user User) ([]byte, error) {
	if b.buf == nil || b.buf.Len() == 0 {
		return nil, fmt.Errorf("empty buffer, transaction has not been executed yet")
	}
	return b.buf.Bytes(), nil
}

// UnmarshalTxResponseBytes accepts the sdk.TxResponse bytes and unmarshalls them into an
// instance of sdk.TxResponse.
func (b *Broadcaster) UnmarshalTxResponseBytes(ctx context.Context, bytes []byte) (sdk.TxResponse, error) {
	resp := sdk.TxResponse{}
	if err := b.chain.cfg.EncodingConfig.Codec.UnmarshalJSON(bytes, &resp); err != nil {
		return sdk.TxResponse{}, err
	}

	// persist nested errors such as ValidateBasic checks.
	code := resp.Code
	rawLog := resp.RawLog

	if code != 0 {
		return resp, fmt.Errorf("error in transaction (code: %d): raw_log: %s", code, rawLog)
	}

	return resp, nil
}

// defaultClientContext returns a default client context configured with the user as the sender.
func (b *Broadcaster) defaultClientContext(fromUser User, sdkAdd sdk.AccAddress) client.Context {
	// initialize a clean buffer each time
	b.buf.Reset()
	kr := b.keyrings[fromUser]
	cn := b.chain.GetFullNode()
	return cn.CliContext().
		WithOutput(b.buf).
		WithFrom(fromUser.FormattedAddress()).
		WithFromAddress(sdkAdd).
		WithFromName(fromUser.KeyName()).
		WithSkipConfirmation(true).
		WithAccountRetriever(AccountRetriever{chain: b.chain}).
		WithKeyring(kr).
		WithBroadcastMode(flags.BroadcastSync).
		WithCodec(b.chain.cfg.EncodingConfig.Codec)

	// NOTE: the returned context used to have .WithHomeDir(cn.Home),
	// but that field no longer exists and the test against Broadcaster still passes without it.
}

// defaultTxFactory creates a new Factory with default configuration.
func (b *Broadcaster) defaultTxFactory(clientCtx client.Context, account client.Account) tx.Factory {
	chainConfig := b.chain.Config()
	return tx.Factory{}.
		WithAccountNumber(account.GetAccountNumber()).
		WithSequence(account.GetSequence()).
		WithSignMode(signing.SignMode_SIGN_MODE_DIRECT).
		WithGasAdjustment(chainConfig.GasAdjustment).
		WithGas(flags.DefaultGasLimit).
		WithGasPrices(chainConfig.GasPrices).
		WithMemo("interchaintest").
		WithTxConfig(clientCtx.TxConfig).
		WithAccountRetriever(clientCtx.AccountRetriever).
		WithKeybase(clientCtx.Keyring).
		WithChainID(clientCtx.ChainID).
		WithSimulateAndExecute(false)
}

// BroadcastTx uses the provided Broadcaster to broadcast all the provided messages which will be signed
// by the User provided. The sdk.TxResponse and an error are returned.
func BroadcastTx(ctx context.Context, broadcaster *Broadcaster, broadcastingUser User, msgs ...sdk.Msg) (sdk.TxResponse, error) {
	f, err := broadcaster.GetFactory(ctx, broadcastingUser)
	if err != nil {
		return sdk.TxResponse{}, err
	}

	cc, err := broadcaster.GetClientContext(ctx, broadcastingUser)
	if err != nil {
		return sdk.TxResponse{}, err
	}

	if err := tx.BroadcastTx(cc, f, msgs...); err != nil {
		return sdk.TxResponse{}, err
	}

	txBytes, err := broadcaster.GetTxResponseBytes(ctx, broadcastingUser)
	if err != nil {
		return sdk.TxResponse{}, err
	}

	err = testutil.WaitForCondition(time.Second*30, time.Second*5, func() (bool, error) {
		var err error
		txBytes, err = broadcaster.GetTxResponseBytes(ctx, broadcastingUser)
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return sdk.TxResponse{}, err
	}

	respWithTxHash, err := broadcaster.UnmarshalTxResponseBytes(ctx, txBytes)
	if err != nil {
		return sdk.TxResponse{}, err
	}

	return getFullyPopulatedResponse(cc, respWithTxHash.TxHash)
}

// getFullyPopulatedResponse returns a fully populated sdk.TxResponse.
// the QueryTx function is periodically called until a tx with the given hash
// has been included in a block.
func getFullyPopulatedResponse(cc client.Context, txHash string) (sdk.TxResponse, error) {
	var resp sdk.TxResponse
	err := testutil.WaitForCondition(time.Second*60, time.Second*5, func() (bool, error) {
		fullyPopulatedTxResp, err := authtx.QueryTx(cc, txHash)
		if err != nil {
			return false, nil
		}

		resp = *fullyPopulatedTxResp
		return true, nil
	})
	return resp, err
}
