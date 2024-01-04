package cosmos

import (
	"context"
	"errors"
	"fmt"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types/v1beta1"

	"github.com/strangelove-ventures/interchaintest/v8/ibc"
	"github.com/strangelove-ventures/interchaintest/v8/testutil"
)

// ConvertProposalStatus converts a proposal status int to a string from IBC-Go v8 / SDK v50 chains.
func ConvertStatus(status int) string {
	return govtypes.ProposalStatus_name[int32(status)]
}

// PollForProposalStatus attempts to find a proposal with matching ID and status using IBC-Go v8 / SDK v50.
func PollForProposalStatusV8(ctx context.Context, chain *CosmosChain, startHeight, maxHeight uint64, proposalID string, status int) (ProposalResponseV8, error) {
	var pr ProposalResponseV8
	doPoll := func(ctx context.Context, height uint64) (ProposalResponseV8, error) {
		p, err := chain.QueryProposalV8(ctx, proposalID)
		if err != nil {
			return pr, err
		}

		if p.Proposal.Status != status {
			return pr, fmt.Errorf("proposal status (%d / %s) does not match expected: (%d / %s)", p.Proposal.Status, ConvertStatus(p.Proposal.Status), status, ConvertStatus(status))
		}
		return *p, nil
	}
	bp := testutil.BlockPoller[ProposalResponseV8]{CurrentHeight: chain.Height, PollFunc: doPoll}
	return bp.DoPoll(ctx, startHeight, maxHeight)
}

// PollForProposalStatus attempts to find a proposal with matching ID and status.
func PollForProposalStatus(ctx context.Context, chain *CosmosChain, startHeight, maxHeight uint64, proposalID string, status string) (ProposalResponse, error) {
	var zero ProposalResponse
	doPoll := func(ctx context.Context, height uint64) (ProposalResponse, error) {
		p, err := chain.QueryProposal(ctx, proposalID)
		if err != nil {
			return zero, err
		}
		if p.Status != status {
			return zero, fmt.Errorf("proposal status (%s) does not match expected: (%s)", p.Status, status)
		}
		return *p, nil
	}
	bp := testutil.BlockPoller[ProposalResponse]{CurrentHeight: chain.Height, PollFunc: doPoll}
	return bp.DoPoll(ctx, startHeight, maxHeight)
}

// PollForMessage searches every transaction for a message. Must pass a coded registry capable of decoding the cosmos transaction.
// fn is optional. Return true from the fn to stop polling and return the found message. If fn is nil, returns the first message to match type T.
func PollForMessage[T any](ctx context.Context, chain *CosmosChain, registry codectypes.InterfaceRegistry, startHeight, maxHeight uint64, fn func(found T) bool) (T, error) {
	var zero T
	if fn == nil {
		fn = func(T) bool { return true }
	}
	doPoll := func(ctx context.Context, height uint64) (T, error) {
		h := int64(height)
		block, err := chain.getFullNode().Client.Block(ctx, &h)
		if err != nil {
			return zero, err
		}
		for _, tx := range block.Block.Txs {
			sdkTx, err := decodeTX(registry, tx)
			if err != nil {
				return zero, err
			}
			for _, msg := range sdkTx.GetMsgs() {
				if found, ok := msg.(T); ok {
					if fn(found) {
						return found, nil
					}
				}
			}
		}
		return zero, errors.New("not found")
	}

	bp := testutil.BlockPoller[T]{CurrentHeight: chain.Height, PollFunc: doPoll}
	return bp.DoPoll(ctx, startHeight, maxHeight)
}

// PollForBalance polls until the balance matches
func PollForBalance(ctx context.Context, chain *CosmosChain, deltaBlocks uint64, balance ibc.WalletAmount) error {
	h, err := chain.Height(ctx)
	if err != nil {
		return fmt.Errorf("failed to get height: %w", err)
	}
	doPoll := func(ctx context.Context, height uint64) (any, error) {
		bal, err := chain.GetBalance(ctx, balance.Address, balance.Denom)
		if err != nil {
			return nil, err
		}
		if !balance.Amount.Equal(bal) {
			return nil, fmt.Errorf("balance (%s) does not match expected: (%s)", bal.String(), balance.Amount.String())
		}
		return nil, nil
	}
	bp := testutil.BlockPoller[any]{CurrentHeight: chain.Height, PollFunc: doPoll}
	_, err = bp.DoPoll(ctx, h, h+deltaBlocks)
	return err
}
