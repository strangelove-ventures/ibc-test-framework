package consensus

import (
	"context"
	"fmt"
	"net/http"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/strangelove-ventures/interchaintest/v8/dockerutil"
)

type Client interface {
	Name() string
	StartFlags(context.Context) string
	IsSynced(ctx context.Context) error
	IsClient(ctx context.Context, img *dockerutil.Image, bin string) bool

	Status(ctx context.Context) (*ctypes.ResultStatus, error)
	BlockResults(ctx context.Context, height *int64) (*ctypes.ResultBlockResults, error)
	Block(ctx context.Context, height *int64) (*ctypes.ResultBlock, error)
	Height(ctx context.Context) (int64, error)
}

// GetBlankClientByName returns a blank client so non state logic (like startup params) can be used.
func NewBlankClient(ctx context.Context, img *dockerutil.Image, bin string) Client {
	clients := []Client{
		&CometBFTClient{},
	}

	for _, client := range clients {
		if client.IsClient(ctx, img, bin) {
			fmt.Printf("NewBlankClient: Found client %s\n", client.Name())
			return client
		}
	}

	fmt.Printf("NewBlankClient: No client found. Defaulting to CometBFT\n")
	return &CometBFTClient{}
}

func NewClientFactory(remote string, client *http.Client) Client {
	cbftClient, err := NewCometBFTClient(remote, client)
	if err != nil {
		panic(err)
	}

	if cbftClient != nil {
		return cbftClient
	}

	panic("NewClientFactory: No client available")
}
