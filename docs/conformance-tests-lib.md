# Conformance Tests

`interchaintest` comes with a suite of conformance tests. These tests ensure IBC and relayer compatibility. On a high-level it tests:
- `client`, `channel`, and `connection` creation
- messages are properly relayed and acknowledged
- packets are being properly timed out

You can view all the specific conformance test by reviewing them in the [conformance](../conformance/) folder.

## Importing Conformance Tests In Your Project

`interchaintest` can be imported into your own packages to be used as a library as well as being used from the
binary itself, see [here](conformance-tests-bin.md).

A common pattern when importing `interchaintest` into your own repositories is to use a Go submodule. The reason being
is that there are some replace directives being used in `interchaintest` which may force your main modules `go.mod`
to use specific versions of dependencies. To avoid this issue one will typically create a new package, such as
`interchaintest` or `e2e`, then you can initialize a new Go module via the `go mod init` command.

## Writing Go Tests For Conformance Testing

The main entrypoint exposed by the `conformance` package is a function named `Test`.

Here is the function signature of `Test`:
```go
func Test(t *testing.T, ctx context.Context, cfs []interchaintest.ChainFactory, rfs []interchaintest.RelayerFactory, rep *testreporter.Reporter)
```

It accepts a normal `testing.T` and `context.Context` from the Go standard library as well as a few types defined in `interchaintest`.

- `testreporter.Reporter` is used for collecting detailed test reports, you can read more about it [here](../testreporter/doc.go).
- `interchaintest.ChainFactory` is used to define which chain pairs should be used in conformance testing.
- `interchaintest.RelayerFactory` is used to define which relayer implementations should be used to test IBC functionality between your chain pairs.

It is important to note that the `Test` function accepts a slice of `ChainFactory`, currently the `conformance` tests only work against
a pair of two chains at a time. This means that each `ChainFactory` should only contain definitions for two chains,
which you will define via the `ChainSpec` type. So if you need to run the `conformance` tests against several different chains
you will need to instantiate several instances of `ChainFactory`.

For our example we will run the `conformance` tests against two different instances of the [Cosmos Hub (Gaia)](https://github.com/cosmos/gaia).

```go

package conformance_test

import (
	"context"
	"testing"

	"github.com/strangelove-ventures/interchaintest/v7"
	"github.com/strangelove-ventures/interchaintest/v7/conformance"
	"github.com/strangelove-ventures/interchaintest/v7/ibc"
	"github.com/strangelove-ventures/interchaintest/v7/testreporter"
	"go.uber.org/zap/zaptest"
)

func TestConformance(t *testing.T) {
	numOfValidators := 2 // Defines how many validators should be used in each network.
	numOfFullNodes := 0  // Defines how many additional full nodes should be used in each network.

	// Here we define our ChainFactory by instantiating a new instance of the BuiltinChainFactory exposed in interchaintest.
	// We use the ChainSpec type to fully describe which chains we want to use in our tests.
	cf := interchaintest.NewBuiltinChainFactory(zaptest.NewLogger(t), []*interchaintest.ChainSpec{
		{
			Name:          "gaia",
			ChainName:     "cosmoshub-1",
			Version:       "v13.0.1",
			NumValidators: &numOfValidators,
			NumFullNodes:  &numOfFullNodes,
		},
		{
			Name:          "gaia",
			ChainName:     "cosmoshub-2",
			Version:       "v13.0.1",
			NumValidators: &numOfValidators,
			NumFullNodes:  &numOfFullNodes,
		},
	})

	// Here we define our RelayerFactory by instantiating a new instance of the BuiltinRelayerFactory exposed in interchaintest.
	// We will instantiate two instances, one for the Go relayer and one for Hermes.
	rlyFactory := interchaintest.NewBuiltinRelayerFactory(
		ibc.CosmosRly,
		zaptest.NewLogger(t),
	)

	hermesFactory := interchaintest.NewBuiltinRelayerFactory(
		ibc.Hermes,
		zaptest.NewLogger(t),
	)

	// conformance.Test requires a Go context
	ctx := context.Background()

	// For our example we will use a No-op reporter that does not actually collect any test reports.
	rep := testreporter.NewNopReporter()

	// Test will now run the conformance test suite against both of our chains, ensuring that they both have basic
	// IBC capabilities properly implemented and work with both the Go relayer and Hermes.
	conformance.Test(t, ctx, []interchaintest.ChainFactory{cf}, []interchaintest.RelayerFactory{rlyFactory, hermesFactory}, rep)
}
```
