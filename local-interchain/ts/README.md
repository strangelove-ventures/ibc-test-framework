# Typescript

Using CosmJS, you can write e2e test on top of interchaintest / local-interchain

## How to run

```bash
# install local-ic if not already
cd local-interchain
make install

# move into the typescript directory
cd ts

npm i

# starts a cosmoshub testnet on :26657
local-ic start cosmoshub

# runs a basic test
npm run start
```