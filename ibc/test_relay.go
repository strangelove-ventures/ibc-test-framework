package ibc

import (
	"fmt"
	"time"

	transfertypes "github.com/cosmos/ibc-go/v3/modules/apps/transfer/types"
)

func (ibc IBCTestCase) RelayPacketTest(testName string, srcChain Chain, dstChain Chain, relayerImplementation RelayerImplementation) error {
	ctx, home, pool, network, cleanup, err := SetupTestRun(testName)
	if err != nil {
		return err
	}
	defer cleanup()

	// startup both chains and relayer
	// creates wallets in the relayer for src and dst chain
	// funds relayer src and dst wallets on respective chain in genesis
	// creates a user account on the src chain (separate fullnode)
	// funds user account on src chain in genesis
	_, channels, srcUser, dstUser, rlyCleanup, err := StartChainsAndRelayer(testName, ctx, pool, network, home, srcChain, dstChain, relayerImplementation, nil)
	if err != nil {
		return err
	}
	defer rlyCleanup()

	// will test a user sending an ibc transfer from the src chain to the dst chain
	// denom will be src chain native denom
	testDenomSrc := srcChain.Config().Denom

	// query initial balance of user wallet for src chain native denom on the src chain
	srcInitialBalance, err := srcChain.GetBalance(ctx, srcUser.SrcChainAddress, testDenomSrc)
	if err != nil {
		return err
	}

	// get ibc denom for test denom on dst chain
	denomTrace := transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom(channels[0].Counterparty.PortID, channels[0].Counterparty.ChannelID, testDenomSrc))
	dstIbcDenom := denomTrace.IBCDenom()

	// query initial balance of user wallet for src chain native denom on the dst chain
	// don't care about error here, account does not exist on destination chain
	dstInitialBalance, _ := dstChain.GetBalance(ctx, srcUser.DstChainAddress, dstIbcDenom)

	fmt.Printf("Initial balances: Src chain: %d\nDst chain: %d\n", srcInitialBalance, dstInitialBalance)

	// test coin, address is recipient of ibc transfer on dst chain
	testCoinSrc := WalletAmount{
		Address: srcUser.DstChainAddress,
		Denom:   testDenomSrc,
		Amount:  1000000,
	}

	// send ibc transfer from the user wallet using its fullnode
	// timeout is nil so that it will use the default timeout
	srcTxHash, err := srcChain.SendIBCTransfer(ctx, channels[0].ChannelID, srcUser.KeyName, testCoinSrc, nil)
	if err != nil {
		return err
	}

	// wait for both chains to produce 10 blocks
	if err := WaitForBlocks(srcChain, dstChain, 10); err != nil {
		return err
	}

	// fetch ibc transfer tx
	srcTx, err := srcChain.GetTransaction(ctx, srcTxHash)
	if err != nil {
		return err
	}

	fmt.Printf("Transaction:\n%v\n", srcTx)

	// query final balance of src user wallet for src chain native denom on the src chain
	srcFinalBalance, err := srcChain.GetBalance(ctx, srcUser.SrcChainAddress, testDenomSrc)
	if err != nil {
		return err
	}

	// query final balance of src user wallet for src chain native denom on the dst chain
	dstFinalBalance, err := dstChain.GetBalance(ctx, srcUser.DstChainAddress, dstIbcDenom)
	if err != nil {
		return err
	}

	totalFees := srcChain.GetGasFeesInNativeDenom(srcTx.GasWanted)
	expectedDifference := testCoinSrc.Amount + totalFees

	if srcFinalBalance != srcInitialBalance-expectedDifference {
		return fmt.Errorf("source balances do not match. expected: %d, actual: %d", srcInitialBalance-expectedDifference, srcFinalBalance)
	}

	if dstFinalBalance != dstInitialBalance+testCoinSrc.Amount {
		return fmt.Errorf("destination balances do not match. expected: %d, actual: %d", dstInitialBalance+testCoinSrc.Amount, dstFinalBalance)
	}

	// Now relay from dst chain to src chain using dst user wallet

	// will test a user sending an ibc transfer from the dst chain to the src chain
	// denom will be dst chain native denom
	testDenomDst := dstChain.Config().Denom

	// query initial balance of dst user wallet for dst chain native denom on the dst chain
	dstInitialBalance, err = dstChain.GetBalance(ctx, dstUser.DstChainAddress, testDenomDst)
	if err != nil {
		return err
	}

	// get ibc denom for test denom on src chain
	srcDenomTrace := transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom(channels[0].PortID, channels[0].ChannelID, testDenomDst))
	srcIbcDenom := srcDenomTrace.IBCDenom()

	// query initial balance of user wallet for src chain native denom on the dst chain
	// don't care about error here, account does not exist on destination chain
	srcInitialBalance, _ = srcChain.GetBalance(ctx, dstUser.SrcChainAddress, srcIbcDenom)

	fmt.Printf("Initial balances: Src chain: %d\nDst chain: %d\n", srcInitialBalance, dstInitialBalance)

	// test coin, address is recipient of ibc transfer on src chain
	testCoinDst := WalletAmount{
		Address: dstUser.SrcChainAddress,
		Denom:   testDenomDst,
		Amount:  1000000,
	}

	// send ibc transfer from the dst user wallet using its fullnode
	// timeout is nil so that it will use the default timeout
	dstTxHash, err := dstChain.SendIBCTransfer(ctx, channels[0].Counterparty.ChannelID, dstUser.KeyName, testCoinDst, nil)
	if err != nil {
		return err
	}

	// wait for both chains to produce 10 blocks
	if err := WaitForBlocks(srcChain, dstChain, 10); err != nil {
		return err
	}

	// fetch ibc transfer tx
	dstTx, err := dstChain.GetTransaction(ctx, dstTxHash)
	if err != nil {
		return err
	}

	fmt.Printf("Transaction:\n%v\n", dstTx)

	// query final balance of dst user wallet for dst chain native denom on the dst chain
	dstFinalBalance, err = dstChain.GetBalance(ctx, dstUser.DstChainAddress, testDenomDst)
	if err != nil {
		return err
	}

	// query final balance of dst user wallet for dst chain native denom on the src chain
	srcFinalBalance, err = srcChain.GetBalance(ctx, dstUser.SrcChainAddress, srcIbcDenom)
	if err != nil {
		return err
	}

	totalFeesDst := dstChain.GetGasFeesInNativeDenom(dstTx.GasWanted)
	expectedDifference = testCoinDst.Amount + totalFeesDst

	if dstFinalBalance != dstInitialBalance-expectedDifference {
		return fmt.Errorf("destination balances do not match. expected: %d, actual: %d", dstInitialBalance-expectedDifference, dstFinalBalance)
	}

	if srcFinalBalance != srcInitialBalance+testCoinDst.Amount {
		return fmt.Errorf("source balances do not match. expected: %d, actual: %d", srcInitialBalance+testCoinDst.Amount, srcFinalBalance)
	}

	return nil
}

// makes sure that a queued packet that is timed out (relative height timeout) will not be relayed
func (ibc IBCTestCase) RelayPacketTestNoTimeout(testName string, srcChain Chain, dstChain Chain, relayerImplementation RelayerImplementation) error {
	ctx, home, pool, network, cleanup, err := SetupTestRun(testName)
	if err != nil {
		return err
	}
	defer cleanup()

	var srcInitialBalance, dstInitialBalance int64
	var txHash string
	testDenom := srcChain.Config().Denom
	var dstIbcDenom string
	var testCoin WalletAmount

	// Query user account balances on both chains and send IBC transfer before starting the relayer
	preRelayerStart := func(channels []ChannelOutput, srcUser User, dstUser User) error {
		var err error
		srcInitialBalance, err = srcChain.GetBalance(ctx, srcUser.SrcChainAddress, testDenom)
		if err != nil {
			return err
		}

		// get ibc denom for test denom on dst chain
		denomTrace := transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom(channels[0].Counterparty.PortID, channels[0].Counterparty.ChannelID, testDenom))
		dstIbcDenom = denomTrace.IBCDenom()

		// don't care about error here, account does not exist on destination chain
		dstInitialBalance, _ = dstChain.GetBalance(ctx, srcUser.DstChainAddress, dstIbcDenom)

		fmt.Printf("Initial balances: Src chain: %d\nDst chain: %d\n", srcInitialBalance, dstInitialBalance)

		testCoin = WalletAmount{
			Address: srcUser.DstChainAddress,
			Denom:   testDenom,
			Amount:  1000000,
		}

		// send ibc transfer with both timeouts disabled
		txHash, err = srcChain.SendIBCTransfer(ctx, channels[0].ChannelID, srcUser.KeyName, testCoin, &IBCTimeout{Height: 0, NanoSeconds: 0})
		return err
	}

	// Startup both chains and relayer
	_, _, user, _, rlyCleanup, err := StartChainsAndRelayer(testName, ctx, pool, network, home, srcChain, dstChain, relayerImplementation, preRelayerStart)
	if err != nil {
		return err
	}
	defer rlyCleanup()

	// wait for both chains to produce 10 blocks
	if err := WaitForBlocks(srcChain, dstChain, 10); err != nil {
		return err
	}

	// fetch ibc transfer tx
	srcTx, err := srcChain.GetTransaction(ctx, txHash)
	if err != nil {
		return err
	}

	fmt.Printf("Transaction:\n%v\n", srcTx)

	srcFinalBalance, err := srcChain.GetBalance(ctx, user.SrcChainAddress, testDenom)
	if err != nil {
		return err
	}

	dstFinalBalance, err := dstChain.GetBalance(ctx, user.DstChainAddress, dstIbcDenom)
	if err != nil {
		return err
	}

	totalFees := srcChain.GetGasFeesInNativeDenom(srcTx.GasWanted)
	expectedDifference := testCoin.Amount + totalFees

	if srcFinalBalance != srcInitialBalance-expectedDifference {
		return fmt.Errorf("source balances do not match. expected: %d, actual: %d", srcInitialBalance-expectedDifference, srcFinalBalance)
	}

	if dstFinalBalance != dstInitialBalance+testCoin.Amount {
		return fmt.Errorf("destination balances do not match. expected: %d, actual: %d", dstInitialBalance+testCoin.Amount, dstFinalBalance)
	}

	return nil
}

// makes sure that a queued packet that is timed out (relative height timeout) will not be relayed
func (ibc IBCTestCase) RelayPacketTestHeightTimeout(testName string, srcChain Chain, dstChain Chain, relayerImplementation RelayerImplementation) error {
	ctx, home, pool, network, cleanup, err := SetupTestRun(testName)
	if err != nil {
		return err
	}
	defer cleanup()

	var srcInitialBalance, dstInitialBalance int64
	var txHash string
	testDenom := srcChain.Config().Denom
	var dstIbcDenom string

	// Query user account balances on both chains and send IBC transfer before starting the relayer
	preRelayerStart := func(channels []ChannelOutput, srcUser User, dstUser User) error {
		var err error
		srcInitialBalance, err = srcChain.GetBalance(ctx, srcUser.SrcChainAddress, testDenom)
		if err != nil {
			return err
		}

		// get ibc denom for test denom on dst chain
		denomTrace := transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom(channels[0].Counterparty.PortID, channels[0].Counterparty.ChannelID, testDenom))
		dstIbcDenom = denomTrace.IBCDenom()

		// don't care about error here, account does not exist on destination chain
		dstInitialBalance, _ = dstChain.GetBalance(ctx, srcUser.DstChainAddress, dstIbcDenom)

		fmt.Printf("Initial balances: Src chain: %d\nDst chain: %d\n", srcInitialBalance, dstInitialBalance)

		testCoin := WalletAmount{
			Address: srcUser.DstChainAddress,
			Denom:   testDenom,
			Amount:  1000000,
		}

		// send ibc transfer with a timeout of 10 blocks from now on counterparty chain
		txHash, err = srcChain.SendIBCTransfer(ctx, channels[0].ChannelID, srcUser.KeyName, testCoin, &IBCTimeout{Height: 10})
		if err != nil {
			return err
		}

		// wait until counterparty chain has passed the timeout
		_, err = dstChain.WaitForBlocks(11)
		return err
	}

	// Startup both chains and relayer
	_, _, user, _, rlyCleanup, err := StartChainsAndRelayer(testName, ctx, pool, network, home, srcChain, dstChain, relayerImplementation, preRelayerStart)
	if err != nil {
		return err
	}
	defer rlyCleanup()

	// wait for both chains to produce 10 blocks
	if err := WaitForBlocks(srcChain, dstChain, 10); err != nil {
		return err
	}

	// fetch ibc transfer tx
	srcTx, err := srcChain.GetTransaction(ctx, txHash)
	if err != nil {
		return err
	}

	fmt.Printf("Transaction:\n%v\n", srcTx)

	srcFinalBalance, err := srcChain.GetBalance(ctx, user.SrcChainAddress, testDenom)
	if err != nil {
		return err
	}

	dstFinalBalance, err := dstChain.GetBalance(ctx, user.DstChainAddress, dstIbcDenom)
	if err != nil {
		return err
	}

	totalFees := srcChain.GetGasFeesInNativeDenom(srcTx.GasWanted)

	if srcFinalBalance != srcInitialBalance-totalFees {
		return fmt.Errorf("source balances do not match. expected: %d, actual: %d", srcInitialBalance-totalFees, srcFinalBalance)
	}

	if dstFinalBalance != dstInitialBalance {
		return fmt.Errorf("destination balances do not match. expected: %d, actual: %d", dstInitialBalance, dstFinalBalance)
	}

	return nil
}

// makes sure that a queued packet that is timed out (nanoseconds timeout) will not be relayed
func (ibc IBCTestCase) RelayPacketTestTimestampTimeout(testName string, srcChain Chain, dstChain Chain, relayerImplementation RelayerImplementation) error {
	ctx, home, pool, network, cleanup, err := SetupTestRun(testName)
	if err != nil {
		return err
	}
	defer cleanup()

	var srcInitialBalance, dstInitialBalance int64
	var txHash string

	testDenom := srcChain.Config().Denom
	var dstIbcDenom string

	// Query user account balances on both chains and send IBC transfer before starting the relayer
	preRelayerStart := func(channels []ChannelOutput, srcUser User, dstUser User) error {
		var err error
		srcInitialBalance, err = srcChain.GetBalance(ctx, srcUser.SrcChainAddress, testDenom)
		if err != nil {
			return err
		}

		// get ibc denom for test denom on dst chain
		denomTrace := transfertypes.ParseDenomTrace(transfertypes.GetPrefixedDenom(channels[0].Counterparty.PortID, channels[0].Counterparty.ChannelID, testDenom))
		dstIbcDenom = denomTrace.IBCDenom()

		// don't care about error here, account does not exist on destination chain
		dstInitialBalance, _ = dstChain.GetBalance(ctx, srcUser.DstChainAddress, dstIbcDenom)

		fmt.Printf("Initial balances: Src chain: %d\nDst chain: %d\n", srcInitialBalance, dstInitialBalance)

		testCoin := WalletAmount{
			Address: srcUser.DstChainAddress,
			Denom:   testDenom,
			Amount:  1000000,
		}

		// send ibc transfer with a timeout of 10 blocks from now on counterparty chain
		txHash, err = srcChain.SendIBCTransfer(ctx, channels[0].ChannelID, srcUser.KeyName, testCoin, &IBCTimeout{NanoSeconds: uint64((10 * time.Second).Nanoseconds())})
		if err != nil {
			return err
		}

		// wait until ibc transfer times out
		time.Sleep(15 * time.Second)

		return nil
	}

	// Startup both chains and relayer
	_, _, user, _, rlyCleanup, err := StartChainsAndRelayer(testName, ctx, pool, network, home, srcChain, dstChain, relayerImplementation, preRelayerStart)
	if err != nil {
		return err
	}
	defer rlyCleanup()

	// wait for both chains to produce 10 blocks
	if err := WaitForBlocks(srcChain, dstChain, 10); err != nil {
		return err
	}

	// fetch ibc transfer tx
	srcTx, err := srcChain.GetTransaction(ctx, txHash)
	if err != nil {
		return err
	}

	fmt.Printf("Transaction:\n%v\n", srcTx)

	srcFinalBalance, err := srcChain.GetBalance(ctx, user.SrcChainAddress, testDenom)
	if err != nil {
		return err
	}

	dstFinalBalance, err := dstChain.GetBalance(ctx, user.DstChainAddress, dstIbcDenom)
	if err != nil {
		return err
	}

	totalFees := srcChain.GetGasFeesInNativeDenom(srcTx.GasWanted)

	if srcFinalBalance != srcInitialBalance-totalFees {
		return fmt.Errorf("source balances do not match. expected: %d, actual: %d", srcInitialBalance-totalFees, srcFinalBalance)
	}

	if dstFinalBalance != dstInitialBalance {
		return fmt.Errorf("destination balances do not match. expected: %d, actual: %d", dstInitialBalance, dstFinalBalance)
	}

	return nil
}
