package features

import (
	"context"
	"fmt"
	"testing"

	tc "github.com/strangelove-ventures/interchaintest/v8/chain/thorchain"
	"github.com/strangelove-ventures/interchaintest/v8/chain/thorchain/common"
	"github.com/strangelove-ventures/interchaintest/v8/ibc"
)

func DualLp(
	t *testing.T,
	ctx context.Context,
	thorchain *tc.Thorchain,
	exoChain ibc.Chain,
) (thorUser ibc.Wallet, exoUser ibc.Wallet, err error) {
	fmt.Println("#### Dual Lper:", exoChain.Config().Name)
	users, err := GetAndFundTestUsers(t, ctx, fmt.Sprintf("%s-DualLper", exoChain.Config().Name), thorchain, exoChain)
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, fund users (%s), %w", exoChain.Config().Name, err)
	}
	thorUser, exoUser = users[0], users[1]

	exoChainType, err := common.NewChain(exoChain.Config().Name)
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, chain type (%s), %w", exoChain.Config().Name, err)
	}
	exoAsset := exoChainType.GetGasAsset()

	thorUserBalance, err := thorchain.GetBalance(ctx, thorUser.FormattedAddress(), thorchain.Config().Denom)
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, thor balance (%s), %w", exoChain.Config().Name, err)
	}
	memo := fmt.Sprintf("+:%s:%s", exoAsset, exoUser.FormattedAddress())
	err = thorchain.Deposit(ctx, thorUser.KeyName(), thorUserBalance.QuoRaw(100).MulRaw(90), thorchain.Config().Denom, memo)
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, thor deposit (%s), %w", exoChain.Config().Name, err)
	}

	exoUserBalance, err := exoChain.GetBalance(ctx, exoUser.FormattedAddress(), exoChain.Config().Denom)
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, exo balance (%s), %w", exoChain.Config().Name, err)
	}
	memo = fmt.Sprintf("+:%s:%s", exoAsset, thorUser.FormattedAddress())
	exoInboundAddr, _, err := thorchain.ApiGetInboundAddress(exoChainType.String())
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, inbound addr (%s), %w", exoChain.Config().Name, err)
	}
	_, err = exoChain.SendFundsWithNote(ctx, exoUser.KeyName(), ibc.WalletAmount{
		Address: exoInboundAddr,
		Denom:   exoChain.Config().Denom,
		Amount:  exoUserBalance.QuoRaw(100).MulRaw(90), // LP 90% of balance
	}, memo)
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, exo send funds (%s), %w", exoChain.Config().Name, err)
	}

	err = PollForPool(ctx, thorchain, 60, exoAsset)
	if err != nil {
		return thorUser, exoUser, fmt.Errorf("duallp, poll for pool (%s), %w", exoChain.Config().Name, err)
	}

	return thorUser, exoUser, err
}
