package cosmos

import (
	"errors"
	"strings"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// AccAddressFromBech32 creates an AccAddress from a Bech32 string.
// https://github.com/cosmos/cosmos-sdk/blob/v0.50.2/types/address.go#L193-L212
func (c *CosmosChain) AccAddressFromBech32(address string) (addr sdk.AccAddress, err error) {
	if len(strings.TrimSpace(address)) == 0 {
		return sdk.AccAddress{}, errors.New("empty address string is not allowed")
	}

	bz, err := sdk.GetFromBech32(address, c.Config().Bech32Prefix)
	if err != nil {
		return nil, err
	}

	err = sdk.VerifyAddressFormat(bz)
	if err != nil {
		return nil, err
	}

	return sdk.AccAddress(bz), nil
}
