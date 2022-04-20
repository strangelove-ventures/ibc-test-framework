package ibc

import (
	"context"
)

type Relayer interface {
	// restore a mnemonic to be used as a relayer wallet for a chain
	RestoreKey(ctx context.Context, chainID, keyName, mnemonic string) error

	// generate a new key
	AddKey(ctx context.Context, chainID, keyName string) (RelayerWallet, error)

	// add relayer configuration for a chain
	AddChainConfiguration(ctx context.Context, chainConfig ChainConfig, keyName, rpcAddr, grpcAddr string) error

	// generate new path between two chains
	GeneratePath(ctx context.Context, srcChainID, dstChainID, pathName string) error

	// setup channels, connections, and clients
	LinkPath(ctx context.Context, pathName string) error

	// update clients, such as after new genesis
	UpdateClients(ctx context.Context, pathName string) error

	// get channel IDs for chain
	GetChannels(ctx context.Context, chainID string) ([]ChannelOutput, error)

	// after configuration is initialized, begin relaying
	StartRelayer(ctx context.Context, pathName string) error

	// relay queue until it is empty
	ClearQueue(ctx context.Context, pathName string, channelID string) error

	// shutdown relayer
	StopRelayer(ctx context.Context) error
}
