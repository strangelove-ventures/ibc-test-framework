# Conformance Tests

`interchaintest` comes with a suite of conformance tests. These tests ensure IBC and relayer compatibility. On a high-level it tests:
- `client`, `channel`, and `connection` creation
- messages are properly relayed and acknowledged 
- packets are being properly timed out

You can view all the specific conformance test by reviewing them in the [conformance](../conformance/) folder.

### Default Environment
To run conformance tests with the default chain and relayer configuration (gaiad <-> osmosisd with the Go Relayer), run the binary without any extra arguments:
```shell
interchaintest
```

To run same tests from source code:
```shell
go test -v ./cmd/interchaintest/
```
### Custom Environment
Using the binary allows for easy custom chain pairs and custom testing environments.

This is accomplished via the `-matrix` argument. 
```shell
interchaintest -matrix <path/to/matrix.json>
```

**Example Matrix Files:**
- [example_matrix.json](../cmd/interchaintest/example_matrix.json) - Basic example using pre-configured chains
- [example_matrix_custom.json](../cmd/interchaintest/example_matrix_custom.json) - More customized example pointing to specific chain binary docker images


By passing in a matrix file you can customize these aspects of the environment:
- chain pairs
- number of validators
- number of full nodes
- relayer tech (currently only integrated with [Go Relayer](https://github.com/cosmos/relayer))


**Pre-Configured Chains**

`interchaintest` comes with [pre-configured chains](../configuredChains.yaml). 
In the matrix file, if `Name` matches the name of any pre-configured chain, `interchaintest` will use standard settings UNLESS overridden in the matrix file. [example_matrix_custom.json](../cmd/interchaintest/example_matrix_custom.json) is an example of overriding all options.


**Custom Binaries**
Chain binaries must be installed in a docker container.
The `Image` array in the matrix json file allows you to pass in docker images with your chain binary of choice. 
If the docker image does not live in a public repository, can you **pass in a local docker image like so:**

```json
        "Images": [
          {
            "Repository": "<DOCKER IMAGE NAME>",
            "Version": "<DOCKER IMAGE TAG>"
          }
        ],
```

If you are supplying custom docker images, you will need to fill out ALL values. See [example_matrix_custom.json](../cmd/interchaintest/example_matrix_custom.json).


Note that the docker images for these pre-configured chains are being pulled from [Heighliner](https://github.com/strangelove-ventures/heighliner) (repository of docker images of many IBC enabled chains). Heighliner needs to have the `Version` you are requesting.


**Logs and block history**


Logs, reports and a SQLite3 database files containing block info will be exported out to `~/.interchaintest/`


## Focusing on Specific Tests

You may focus on a specific tests using the `-test.run=<regex>` flag.

```shell
interchaintest -test.run=/<test category>/<chain combination>/<relayer>/<test subcategory>/<test name>
```

If you want to focus on a specific test:

```shell
interchaintest -test.run=/////relay_packet
interchaintest -test.run=/////no_timeout
interchaintest -test.run=/////height_timeout
interchaintest -test.run=/////timestamp_timeout
```

Example of narrowing your focus even more:

```shell
# run all tests for Go relayer
interchaintest -test.run=///rly/

# run all tests for Go relayer and gaia chains
interchaintest -test.run=//gaia/rly/

# only run no_timeout test for Go relayer and gaia chains
interchaintest -test.run=//gaia/rly/conformance/no_timeout
```