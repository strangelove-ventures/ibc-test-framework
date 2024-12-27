# Chains Examples

## interchainsecurity & neutron

Currently the neutron heighliner image does not work due to some /tmp issue in the docker image. For this reason, you must compile the image yourself to use the latest v50 instance.

```bash
git clone https://github.com/neutron-org/neutron.git --depth 1 --branch v4.2.1
cd neutron

# neutron-node:latest
make build-e2e-docker-image
```
