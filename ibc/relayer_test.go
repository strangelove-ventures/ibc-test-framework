package ibc

import (
	"testing"

	chantypes "github.com/cosmos/ibc-go/v8/modules/core/04-channel/types"
	"github.com/stretchr/testify/require"
)

func TestChannelOptsConfigured(t *testing.T) {
	// Test the default channel opts
	opts := DefaultChannelOpts()
	require.NoError(t, opts.Validate())

	// Test empty struct channel opts
	opts = CreateChannelOptions{}
	require.Error(t, opts.Validate())

	// Test invalid Order type in channel opts
	opts = CreateChannelOptions{
		SourcePortName: "transfer",
		DestPortName:   "transfer",
		Order:          3,
		Version:        "123",
	}
	require.Error(t, opts.Validate())
	require.Equal(t, chantypes.ErrInvalidChannelOrdering, opts.Order.Validate())

	// Test partial channel opts
	opts = CreateChannelOptions{
		SourcePortName: "",
		DestPortName:   "",
		Order:          0,
	}
	require.Error(t, opts.Validate())
}
