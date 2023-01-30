# Write Custom Tests

This document breaks down code snippets from [learn_ibc_test.go](../examples/ibc/learn_ibc_test.go). This test:

1) Spins up two chains (Gaia and Osmosis) 
2) Creates an IBC Path between them (client, connection, channel)
3) Sends an IBC transaction between them.

It validates each step and confirms that the balances of each wallet are correct. 


### Three basic components of `interchaintest`:
- **Chain Factory** - Select chain binaries to include in tests
- **Relayer Factory** - Select Relayer to use in tests
- **Interchain** - Where the testnet is configured and spun up


## Chain Factory

```go
cf := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
    {Name: "gaia", Version: "v7.0.1", ChainConfig: ibc.ChainConfig{
        GasPrices: "0.0uatom",
    }},
    {Name: "osmosis", Version: "v11.0.1"},
})
```

The chain factory is where you configure your chain binaries. 

`interchaintest` needs a docker image with the chain binary(s) installed to spin up the local testnet. 

`interchaintest` has several [pre-configured chains](../configuredChains.yaml). These docker images are pulled from [Heighliner](https://github.com/strangelove-ventures/heighliner) (repository of docker images of many IBC enabled chains). Note that Heighliner needs to have the `Version` you are requesting.

When creating your `ChainFactory`, if the `Name` matches the name of a pre-configured chain, the pre-configured settings are used. You can override these settings by passing them into the `ibc.ChainConfig` when initializing your ChainFactory. We do this above with `GasPrices` for gaia.

You can also pass in **remote images** and/or **local docker images**. 

See an examples below:

```go
cf := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
    
    // -- PRE CONFIGURED CHAIN EXAMPLE --
    {Name: "gaia", Version: "v7.0.2"},

    // -- REMOTE/LOCAL IMAGE EXAMPLE --
    {ChainConfig: ibc.ChainConfig{
        Type: "cosmos",
        Name: "ibc-go-simd",
        ChainID: "simd-1",
        Images: []ibc.DockerImage{
            {
                Repository: "ghcr.io/cosmos/ibc-go-simd-e2e",   // FOR LOCAL IMAGE USE: Docker Image Name
                Version: "pr-1973",                             // FOR LOCAL IMAGE USE: Docker Image Tag  
            },
        },
        Bin: "simd",
        Bech32Prefix: "cosmos",
        Denom: "gos",
        GasPrices: "0.00gos",
        GasAdjustment: 1.3,
        TrustingPeriod: "508h",
        NoHostMount: false},
    },
    })
```
If you are not using a pre-configured chain, you must fill out all values of the `interchaintest.ChainSpec`.


By default, `interchaintest` will spin up a 3 docker images for each chain:
- 2 validator nodes
- 1 full node. 

These settings can all be configured inside the `ChainSpec`.

EXAMPLE: Overriding defaults for number of validators and full nodes in `ChainSpec`:

```go
gaiaValidators := int(4)
gaiaFullnodes := int(2)
cf := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
    {Name: "gaia", ChainName: "gaia", Version: "v7.0.2", NumValidators: &gaiaValidators, NumFullNodes: &gaiaFullnodes},
})
```

Here we break out each chain in preparation to pass into `Interchain` (documented below):
```go
chains, err := cf.Chains(t.Name())
require.NoError(t, err)
gaia, osmosis := chains[0], chains[1]
```

## Relayer Factory

The relayer factory is where relayer docker images are configured. 

Currently only the [Cosmos/Relayer](https://github.com/cosmos/relayer)(CosmosRly) is integrated into `interchaintest`. 

Here we prep an image with the Cosmos/Relayer:
```go
client, network := interchaintest.DockerSetup(t)
r := interchaintest.NewBuiltinRelayerFactory(ibc.CosmosRly, zaptest.NewLogger(t)).Build(
    t, client, network)
```

## Interchain

This is where we configure our test-net/interchain. 

We prep the "interchain" by adding chains, a relayer, and specifying which chains to create IBC paths for:

```go
const ibcPath = "gaia-osmosis-demo"
ic := interchaintest.NewInterchain().
    AddChain(gaia).
    AddChain(osmosis).
    AddRelayer(r, "relayer").
    AddLink(interchaintest.InterchainLink{
        Chain1:  gaia,
        Chain2:  osmosis,
        Relayer: r,
        Path:    ibcPath,
    })
```

The `Build` function below spins everything up.

```go
rep := testreporter.NewReporter(f) // f is the path to output reports
eRep := rep.RelayerExecReporter(t)
require.NoError(t, ic.Build(ctx, eRep, interchaintest.InterchainBuildOptions{
    TestName:  t.Name(),
    Client:    client,
    NetworkID: network,
    BlockDatabaseFile: interchaintest.DefaultBlockDatabaseFilepath(),

    SkipPathCreation: false,
}))
```

Upon calling build, several things happen (specifically for cosmos based chains):

- Each validator gets 2 trillion units of "stake" funded in genesis
    - 1 trillion "stake" are staked
    - 100 billion "stake" are self delegated
- Each chain gets a faucet address (key named "faucet") with 10 billion units of denom funded in genesis
- The relayer wallet gets 1 billion units of each chains denom funded in genesis 
- Genesis for each chain takes place
- IBC paths are created: `client`, `connection`, `channel` for each link


Note that this function takes a `testReporter`. This will instruct `interchaintest` to export and reports of the test(s). The `RelayerExecReporter` satisfies the reporter requirement. 

Note: If report files are not needed, you can use `testreporter.NewNopReporter()` instead.
    

Passing in the optional `BlockDatabaseFile` will instruct `interchaintest` to create a sqlite3 database with all block history. This includes raw event data.


Unless specified, default options are used for `client`, `connection`, and `channel` creation. 


Default `createChannelOptions` are:
```yaml
    SourcePortName: "transfer",
    DestPortName:   "transfer",
    Order:          Unordered,
    Version:        "ics20-1",
```

EXAMPLE: Passing in channel options to support the `ics27-1` interchain accounts standard:
```go
require.NoError(t, ic.Build(ctx, eRep, interchaintest.InterchainBuildOptions{
		TestName:  t.Name(),
		Client:    client,
		NetworkID: network,

		CreateChannelOpts: ibc.CreateChannelOptions{
			SourcePortName: "transfer",
			DestPortName:   "transfer",
			Order:          ibc.Ordered,
			Version:        "ics27-1",
		},

		SkipPathCreation: false},
	),
	)
```

Note the `SkipPathCreation` boolean. You can set this to `true` if IBC paths (`client`, `connection` and `channel`) are not necessary OR if you would like to make those calls manually.


## Creating Users(wallets)

Here we create new funded wallets(users) for both chains. These wallets are funded from the "faucet" key created at genesis.
Note that there is also the option to restore a wallet (`interchaintest.GetAndFundTestUserWithMnemonic`)

```go
fundAmount := int64(10_000_000)
users := interchaintest.GetAndFundTestUsers(t, ctx, "default", int64(fundAmount), gaia, osmosis)
gaiaUser := users[0]
osmosisUser := users[1]
```

## Interacting with the Interchain

Now that the interchain is built, you can interact with each binary. 

EXAMPLE: Getting the RPC address:
```go
gaiaRPC := gaia.GetGRPCAddress()
osmosisRPC := osmosis.GetGRPCAddress()
```

Here we send an IBC Transaction:
```go
amountToSend := int64(1_000_000)
transfer := ibc.WalletAmount{
    Address: osmosisUser.Bech32Address(osmosis.Config().Bech32Prefix),
    Denom:   gaia.Config().Denom,
    Amount:  amountToSend,
}
tx, err := gaia.SendIBCTransfer(ctx, gaiaChannelID, gaiaUser.KeyName, transfer, ibc.TransferOptions{})
```

The `Exec` method allows any arbitrary command to be passed into a chain binary or relayer binary. 

EXAMPLE: Sending an IBC transfer with the `Exec`:
```go
	amountToSendString := strconv.Itoa(int(amountToSend)) + gaia.Config().Denom
	cmd := []string{gaia.Config().Bin, "tx", "ibc-transfer", "transfer", "transfer", gaiaChannelID, dstAddress,
		amountToSendString,
		"--keyring-backend", keyring.BackendTest,
		"--node", gaia.GetRPCAddress(),
		"--from", gaiaUser.KeyName,
		"--gas-prices", gaia.Config().GasPrices,
		"--home", gaia.HomeDir(),
		"--chain-id", gaia.Config().ChainID,
	}
	_, _, err = gaia.Exec(ctx, cmd, nil)
	require.NoError(t, err)

	testutil.WaitForBlocks(ctx, 3, gaia)
```
Notice, how it waits for blocks. Sometimes this is necessary.


Here we instruct the relayer to flush packets and acknowledgments.

```go
require.NoError(t, r.FlushPackets(ctx, eRep, ibcPath, osmoChannelID))
require.NoError(t, r.FlushAcknowledgements(ctx, eRep, ibcPath, gaiaChannelID))
```

This could have also been accomplished by starting the relayer on a loop:

```go
require.NoError(t, r.StartRelayer(ctx, eRep, ibcPath))
testutil.WaitForBlocks(ctx, 3, gaia)
```

## Final Notes
When troubleshooting while writing tests, it can be helpful to print out variables:
```go
t.log("PRINT STATEMENT: ", variableToPrint)
```
You will need to pass in the `-v` flag in the `go test` command to see this output. Exampled below.


This document only scratches the surface of the full functionality of `interchaintest`. Refer to other tests in this repo for more in-depth/advanced testing examples.


## How to run

Running tests leverages Go Tests.

For more details, run:

`go help test`

In general, your test needs to be in a file ending in "_test.go". The function name must start with "Test"

To run:

`go test -timeout 10m -v -run <NAME_OF_TEST> <PATH/TO/FOLDER/HOUSING/TEST/FILES>`
<br>

---

<br>
Happy Testing 🧪