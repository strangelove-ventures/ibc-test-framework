package penumbra

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	volumetypes "github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	cryptov1alpha1 "github.com/strangelove-ventures/interchaintest/v7/chain/penumbra/core/crypto/v1alpha1"
	custodyv1alpha1 "github.com/strangelove-ventures/interchaintest/v7/chain/penumbra/custody/v1alpha1"
	viewv1alpha1 "github.com/strangelove-ventures/interchaintest/v7/chain/penumbra/view/v1alpha1"
	"github.com/strangelove-ventures/interchaintest/v7/ibc"
	"github.com/strangelove-ventures/interchaintest/v7/internal/dockerutil"
	"github.com/strangelove-ventures/interchaintest/v7/testutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type PenumbraClientNode struct {
	log *zap.Logger

	KeyName      string
	Index        int
	VolumeName   string
	Chain        ibc.Chain
	TestName     string
	NetworkID    string
	DockerClient *client.Client
	Image        ibc.DockerImage

	containerLifecycle *dockerutil.ContainerLifecycle

	// Set during StartContainer.
	hostGRPCPort string
}

func NewClientNode(
	ctx context.Context,
	log *zap.Logger,
	chain *PenumbraChain,
	keyName string,
	index int,
	testName string,
	image ibc.DockerImage,
	dockerClient *client.Client,
	networkID string,
) (*PenumbraClientNode, error) {
	p := &PenumbraClientNode{
		log:          log,
		KeyName:      keyName,
		Index:        index,
		Chain:        chain,
		TestName:     testName,
		Image:        image,
		DockerClient: dockerClient,
		NetworkID:    networkID,
	}

	p.containerLifecycle = dockerutil.NewContainerLifecycle(log, dockerClient, p.Name())

	tv, err := dockerClient.VolumeCreate(ctx, volumetypes.VolumeCreateBody{
		Labels: map[string]string{
			dockerutil.CleanupLabel: testName,

			dockerutil.NodeOwnerLabel: p.Name(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating pclientd volume: %w", err)
	}
	p.VolumeName = tv.Name
	if err := dockerutil.SetVolumeOwner(ctx, dockerutil.VolumeOwnerOptions{
		Log: log,

		Client: dockerClient,

		VolumeName: p.VolumeName,
		ImageRef:   image.Ref(),
		TestName:   testName,
		UidGid:     image.UidGid,
	}); err != nil {
		return nil, fmt.Errorf("set pclientd volume owner: %w", err)
	}

	return p, nil
}

const (
	pclientdPort = "8081/tcp"
)

var pclientdPorts = nat.PortSet{
	nat.Port(pclientdPort): {},
}

// Name of the test node container
func (p *PenumbraClientNode) Name() string {
	return fmt.Sprintf("pclientd-%d-%s-%s-%s", p.Index, p.KeyName, p.Chain.Config().ChainID, p.TestName)
}

// the hostname of the test node container
func (p *PenumbraClientNode) HostName() string {
	return dockerutil.CondenseHostName(p.Name())
}

// Bind returns the home folder bind point for running the node
func (p *PenumbraClientNode) Bind() []string {
	return []string{fmt.Sprintf("%s:%s", p.VolumeName, p.HomeDir())}
}

func (p *PenumbraClientNode) HomeDir() string {
	return "/home/heighliner"
}

func (p *PenumbraClientNode) GetAddress(ctx context.Context) ([]byte, error) {
	// TODO make grpc call to pclientd to get address
	panic("not yet implemented")
}

func (p *PenumbraClientNode) SendFunds(ctx context.Context, amount ibc.WalletAmount) error {
	channel, err := grpc.Dial(fmt.Sprintf(p.HostName()))
	if err != nil {
		return err
	}
	defer channel.Close()

	// 5.1. Generate a transaction plan sending funds to an address.
	tpr := &viewv1alpha1.TransactionPlannerRequest{
		XAccountGroupId: nil,
		Outputs: []*viewv1alpha1.TransactionPlannerRequest_Output{{
			Value: &cryptov1alpha1.Value{
				Amount: &cryptov1alpha1.Amount{
					Lo: uint64(amount.Amount),
					Hi: uint64(amount.Amount),
				},
				AssetId: &cryptov1alpha1.AssetId{Inner: []byte(amount.Denom)},
			},
			Address: &cryptov1alpha1.Address{Inner: []byte(amount.Address)},
		}},
	}

	viewClient := viewv1alpha1.NewViewProtocolServiceClient(channel)
	resp, err := viewClient.TransactionPlanner(ctx, tpr)
	if err != nil {
		return err
	}

	// 5.2. Get authorization data for the transaction from pclientd (signing).
	custodyClient := custodyv1alpha1.NewCustodyProtocolServiceClient(channel)

	authorizeReq := &custodyv1alpha1.AuthorizeRequest{
		Plan:              resp.Plan,
		AccountGroupId:    nil,
		PreAuthorizations: nil,
	}
	authData, err := custodyClient.Authorize(ctx, authorizeReq, nil)
	if err != nil {
		return err
	}

	// 5.3. Have pclientd build and sign the planned transaction.
	wbr := &viewv1alpha1.WitnessAndBuildRequest{
		TransactionPlan:   resp.Plan,
		AuthorizationData: authData.Data,
	}

	tx, err := viewClient.WitnessAndBuild(ctx, wbr, nil)
	if err != nil {
		return err
	}

	// 5.4. Have pclientd broadcast and await confirmation of the built transaction.
	btr := &viewv1alpha1.BroadcastTransactionRequest{
		Transaction:    tx.Transaction,
		AwaitDetection: false,
	}

	_, err = viewClient.BroadcastTransaction(ctx, btr, nil)
	if err != nil {
		return err
	}

	return nil
}

func (p *PenumbraClientNode) SendIBCTransfer(
	ctx context.Context,
	channelID string,
	amount ibc.WalletAmount,
	options ibc.TransferOptions,
) (ibc.Tx, error) {
	// TODO make grpc call to pclientd to send ibc transfer
	panic("not yet implemented")
}

func (p *PenumbraClientNode) GetBalance(ctx context.Context, denom string) (int64, error) {
	//channel, err := grpc.Dial(fmt.Sprintf(p.HostName()))
	//if err != nil {
	//	return 0, err
	//}
	//defer channel.Close()
	//
	//viewClient := viewv1alpha1.NewViewProtocolServiceClient(channel)
	//resp, err := viewClient.BalanceByAddress()
	//if err != nil {
	//	return 0, err
	//}
	//
	//bal, err := resp.Recv()
	//if err != nil {
	//	return 0, err
	//}
	//
	//return int64(bal.Amount.Hi), nil
	// TODO implement
	panic("implement me")
}

// WriteFile accepts file contents in a byte slice and writes the contents to
// the docker filesystem. relPath describes the location of the file in the
// docker volume relative to the home directory
func (p *PenumbraClientNode) WriteFile(ctx context.Context, content []byte, relPath string) error {
	fw := dockerutil.NewFileWriter(p.log, p.DockerClient, p.TestName)
	return fw.WriteFile(ctx, p.VolumeName, relPath, content)
}

// Initialize loads the view and spend keys into the pclientd config.
func (p *PenumbraClientNode) Initialize(ctx context.Context, spendKey, fullViewingKey string) error {
	c := make(testutil.Toml)

	kmsConfig := make(testutil.Toml)
	kmsConfig["spend_key"] = spendKey
	c["kms_config"] = kmsConfig
	c["fvk"] = fullViewingKey

	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(c); err != nil {
		return err
	}

	return p.WriteFile(ctx, buf.Bytes(), "config.toml")
}

func (p *PenumbraClientNode) CreateNodeContainer(ctx context.Context, pdAddress string) error {
	cmd := []string{
		"pclientd",
		"--home", p.HomeDir(),
		"--node", pdAddress,
		"start",
		"--bind-addr", "0.0.0.0:" + strings.Split(pclientdPort, "/")[0],
	}

	return p.containerLifecycle.CreateContainer(ctx, p.TestName, p.NetworkID, p.Image, pclientdPorts, p.Bind(), p.HostName(), cmd)
}

func (p *PenumbraClientNode) StopContainer(ctx context.Context) error {
	return p.containerLifecycle.StopContainer(ctx)
}

func (p *PenumbraClientNode) StartContainer(ctx context.Context) error {
	if err := p.containerLifecycle.StartContainer(ctx); err != nil {
		return err
	}

	hostPorts, err := p.containerLifecycle.GetHostPorts(ctx, pclientdPort)
	if err != nil {
		return err
	}

	p.hostGRPCPort = hostPorts[0]

	return nil
}

// Exec run a container for a specific job and block until the container exits
func (p *PenumbraClientNode) Exec(ctx context.Context, cmd []string, env []string) ([]byte, []byte, error) {
	job := dockerutil.NewImage(p.log, p.DockerClient, p.NetworkID, p.TestName, p.Image.Repository, p.Image.Version)
	opts := dockerutil.ContainerOptions{
		Binds: p.Bind(),
		Env:   env,
		User:  p.Image.UidGid,
	}
	res := job.Run(ctx, cmd, opts)
	return res.Stdout, res.Stderr, res.Err
}
