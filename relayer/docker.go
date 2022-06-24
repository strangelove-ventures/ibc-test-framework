package relayer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/strangelove-ventures/ibctest/ibc"
	"github.com/strangelove-ventures/ibctest/internal/dockerutil"
	"go.uber.org/zap"
)

// DockerRelayer provides a common base for relayer implementations
// that run on Docker.
type DockerRelayer struct {
	log *zap.Logger

	// c defines all the commands to run inside the container.
	c RelayerCommander

	home      string
	pool      *dockertest.Pool
	networkID string

	testName string

	customImage *ibc.DockerImage
	pullImage   bool

	// The container created by StartRelayer.
	container *docker.Container

	didInit bool

	// wallets contains a mapping of chainID to relayer wallet
	wallets map[string]ibc.RelayerWallet
}

var _ ibc.Relayer = (*DockerRelayer)(nil)

// NewDockerRelayer returns a new DockerRelayer.
func NewDockerRelayer(log *zap.Logger, testName, home string, pool *dockertest.Pool, networkID string, c RelayerCommander, options ...RelayerOption) *DockerRelayer {
	relayer := DockerRelayer{
		log: log,

		c: c,

		home:      home,
		pool:      pool,
		networkID: networkID,

		// pull true by default, can be overridden with options
		pullImage: true,

		testName: testName,

		wallets: map[string]ibc.RelayerWallet{},
	}

	for _, opt := range options {
		switch typedOpt := opt.(type) {
		case RelayerOptionDockerImage:
			relayer.customImage = &typedOpt.DockerImage
		case RelayerOptionImagePull:
			relayer.pullImage = typedOpt.Pull
		}
	}

	containerImage := relayer.containerImage()
	if err := relayer.pullContainerImageIfNecessary(containerImage); err != nil {
		log.Error("Error pulling container image", zap.String("repository", containerImage.Repository), zap.String("version", containerImage.Version), zap.Error(err))
	}

	return &relayer
}

func (r *DockerRelayer) AddChainConfiguration(ctx context.Context, rep ibc.RelayerExecReporter, chainConfig ibc.ChainConfig, keyName, rpcAddr, grpcAddr string) error {
	// rly needs to run "rly config init", and AddChainConfiguration should be the first call where it's needed.
	// This might be a better fit for NewDockerRelayer, but that would considerably change the function signature.
	if !r.didInit {
		if init := r.c.Init(r.NodeHome()); len(init) > 0 {
			// Initialization should complete immediately,
			// but add a 1-minute timeout in case Docker hangs on a developer workstation.
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()

			exitCode, stdout, stderr, err := r.NodeJob(ctx, rep, init)
			if err != nil {
				return fmt.Errorf("relayer initialization failed: %w", dockerutil.HandleNodeJobError(exitCode, stdout, stderr, err))
			}
		}

		r.didInit = true
	}

	// For rly this file is json, but the file extension should not matter.
	// Using .config to avoid implying
	chainConfigFile := chainConfig.ChainID + ".config"

	chainConfigLocalFilePath := filepath.Join(r.Dir(), chainConfigFile)
	chainConfigContainerFilePath := fmt.Sprintf("%s/%s", r.NodeHome(), chainConfigFile)

	configContent, err := r.c.ConfigContent(ctx, chainConfig, keyName, rpcAddr, grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to generate config content: %w", err)
	}

	if err := os.WriteFile(chainConfigLocalFilePath, configContent, 0644); err != nil {
		return fmt.Errorf("failed to write config to host disk: %w", err)
	}

	cmd := r.c.AddChainConfiguration(chainConfigContainerFilePath, r.NodeHome())

	// Adding the chain configuration simply reads from a file on disk,
	// so this should also complete immediately.
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) AddKey(ctx context.Context, rep ibc.RelayerExecReporter, chainID, keyName string) (ibc.RelayerWallet, error) {
	cmd := r.c.AddKey(chainID, keyName, r.NodeHome())

	// Adding a key should be near-instantaneous, so add a 1-minute timeout
	// to detect if Docker has hung.
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	exitCode, stdout, stderr, err := r.NodeJob(ctx, rep, cmd)
	if err != nil {
		return ibc.RelayerWallet{}, dockerutil.HandleNodeJobError(exitCode, stdout, stderr, err)
	}

	wallet, err := r.c.ParseAddKeyOutput(stdout, stderr)
	if err != nil {
		return ibc.RelayerWallet{}, dockerutil.HandleNodeJobError(exitCode, stdout, stderr, err)
	}
	r.wallets[chainID] = wallet
	return wallet, nil
}

func (r *DockerRelayer) GetWallet(chainID string) (ibc.RelayerWallet, bool) {
	wallet, ok := r.wallets[chainID]
	return wallet, ok
}

func (r *DockerRelayer) CreateChannel(ctx context.Context, rep ibc.RelayerExecReporter, pathName string, opts ibc.CreateChannelOptions) error {
	cmd := r.c.CreateChannel(pathName, opts, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) CreateClients(ctx context.Context, rep ibc.RelayerExecReporter, pathName string) error {
	cmd := r.c.CreateClients(pathName, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) CreateConnections(ctx context.Context, rep ibc.RelayerExecReporter, pathName string) error {
	cmd := r.c.CreateConnections(pathName, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) FlushAcknowledgements(ctx context.Context, rep ibc.RelayerExecReporter, pathName, channelID string) error {
	cmd := r.c.FlushAcknowledgements(pathName, channelID, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) FlushPackets(ctx context.Context, rep ibc.RelayerExecReporter, pathName, channelID string) error {
	cmd := r.c.FlushPackets(pathName, channelID, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) GeneratePath(ctx context.Context, rep ibc.RelayerExecReporter, srcChainID, dstChainID, pathName string) error {
	cmd := r.c.GeneratePath(srcChainID, dstChainID, pathName, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) GetChannels(ctx context.Context, rep ibc.RelayerExecReporter, chainID string) ([]ibc.ChannelOutput, error) {
	cmd := r.c.GetChannels(chainID, r.NodeHome())

	// Getting channels should be very quick, but go up to a 3-minute timeout just in case.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	exitCode, stdout, stderr, err := r.NodeJob(ctx, rep, cmd)
	if err != nil {
		return nil, dockerutil.HandleNodeJobError(exitCode, stdout, stderr, err)
	}

	return r.c.ParseGetChannelsOutput(stdout, stderr)
}

func (r *DockerRelayer) GetConnections(ctx context.Context, rep ibc.RelayerExecReporter, chainID string) (ibc.ConnectionOutputs, error) {
	cmd := r.c.GetConnections(chainID, r.NodeHome())
	exitCode, stdout, stderr, err := r.NodeJob(ctx, rep, cmd)
	if err != nil {
		return nil, dockerutil.HandleNodeJobError(exitCode, stdout, stderr, err)
	}

	return r.c.ParseGetConnectionsOutput(stdout, stderr)
}

func (r *DockerRelayer) LinkPath(ctx context.Context, rep ibc.RelayerExecReporter, pathName string) error {
	cmd := r.c.LinkPath(pathName, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) RestoreKey(ctx context.Context, rep ibc.RelayerExecReporter, chainID, keyName, mnemonic string) error {
	cmd := r.c.RestoreKey(chainID, keyName, mnemonic, r.NodeHome())

	// Restoring a key should be near-instantaneous, so add a 1-minute timeout
	// to detect if Docker has hung.
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	exitCode, stdout, stderr, err := r.NodeJob(ctx, rep, cmd)
	if err != nil {
		return dockerutil.HandleNodeJobError(exitCode, stdout, stderr, err)
	}

	r.wallets[chainID] = ibc.RelayerWallet{
		Mnemonic: mnemonic,
		Address:  r.c.ParseRestoreKeyOutput(stdout, stdout),
	}
	return nil
}

func (r *DockerRelayer) UpdateClients(ctx context.Context, rep ibc.RelayerExecReporter, pathName string) error {
	cmd := r.c.UpdateClients(pathName, r.NodeHome())
	return dockerutil.HandleNodeJobError(r.NodeJob(ctx, rep, cmd))
}

func (r *DockerRelayer) StartRelayer(ctx context.Context, rep ibc.RelayerExecReporter, pathName string) error {
	return r.createNodeContainer(pathName)
}

func (r *DockerRelayer) StopRelayer(ctx context.Context, rep ibc.RelayerExecReporter) error {
	if err := r.stopContainer(); err != nil {
		return err
	}
	finishedAt := time.Now()

	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)
	_ = r.pool.Client.Logs(docker.LogsOptions{
		Context:      ctx,
		Container:    r.container.ID,
		OutputStream: stdoutBuf,
		ErrorStream:  stderrBuf,
		Stdout:       true,
		Stderr:       true,
		Tail:         "50",
		Follow:       false,
		Timestamps:   false,
	})

	stdout := stdoutBuf.String()
	stderr := stderrBuf.String()

	rep.TrackRelayerExec(
		r.container.Name,
		r.container.Args, // Is this correct for the command?
		stdout, stderr,
		r.container.State.ExitCode, // Is this accurate?
		r.container.Created,
		finishedAt,
		nil,
	)

	r.log.Debug(
		fmt.Sprintf("Stopped docker container\nstdout:\n%s\nstderr:\n%s", stdout, stderr),
		zap.String("container_id", r.container.ID),
		zap.String("container", r.container.Name),
	)

	return r.pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: r.container.ID})
}

func (r *DockerRelayer) containerImage() ibc.DockerImage {
	if r.customImage != nil {
		return *r.customImage
	}
	return ibc.DockerImage{
		Repository: r.c.DefaultContainerImage(),
		Version:    r.c.DefaultContainerVersion(),
	}
}

func (r *DockerRelayer) pullContainerImageIfNecessary(containerImage ibc.DockerImage) error {
	if !r.pullImage {
		return nil
	}
	return r.pool.Client.PullImage(docker.PullImageOptions{
		Repository: containerImage.Repository,
		Tag:        containerImage.Version,
	}, docker.AuthConfiguration{})
}

func (r *DockerRelayer) createNodeContainer(pathName string) error {
	containerImage := r.containerImage()
	containerName := fmt.Sprintf("%s-%s", r.c.Name(), pathName)
	cmd := r.c.StartRelayer(pathName, r.NodeHome())
	r.log.Info(
		"Running command",
		zap.String("command", strings.Join(cmd, " ")),
		zap.String("container", containerName),
	)
	cont, err := r.pool.Client.CreateContainer(docker.CreateContainerOptions{
		Name: containerName,
		Config: &docker.Config{
			User:       dockerutil.GetDockerUserString(),
			Cmd:        cmd,
			Entrypoint: []string{},
			Hostname:   r.HostName(pathName),
			Image:      fmt.Sprintf("%s:%s", containerImage.Repository, containerImage.Version),
			Labels:     map[string]string{dockerutil.CleanupLabel: r.testName},
		},
		NetworkingConfig: &docker.NetworkingConfig{
			EndpointsConfig: map[string]*docker.EndpointConfig{
				r.networkID: {},
			},
		},
		HostConfig: &docker.HostConfig{
			Binds:      r.Bind(),
			AutoRemove: false,
		},
	})
	if err != nil {
		return err
	}
	r.container = cont
	return r.pool.Client.StartContainer(r.container.ID, nil)
}

func (r *DockerRelayer) NodeJob(ctx context.Context, rep ibc.RelayerExecReporter, cmd []string) (
	exitCode int,
	stdout, stderr string,
	err error,
) {
	startedAt := time.Now()
	var container string
	defer func() {
		rep.TrackRelayerExec(
			container,
			cmd,
			stdout, stderr,
			exitCode,
			startedAt, time.Now(),
			err,
		)
	}()
	containerImage := r.containerImage()
	counter, _, _, _ := runtime.Caller(1)
	caller := runtime.FuncForPC(counter).Name()
	funcName := strings.Split(caller, ".")
	container = fmt.Sprintf("%s-%s-%s", r.Name(), funcName[len(funcName)-1], dockerutil.RandLowerCaseLetterString(3))

	r.log.Info(
		"Running command",
		zap.String("command", strings.Join(cmd, " ")),
		zap.String("container", container),
	)

	cont, err := r.pool.Client.CreateContainer(docker.CreateContainerOptions{
		Context: ctx,
		Name:    container,
		Config: &docker.Config{
			User: dockerutil.GetDockerUserString(),
			// random hostname is fine here, just for setup
			Hostname:   dockerutil.CondenseHostName(container),
			Image:      containerImage.Repository + ":" + containerImage.Version,
			Cmd:        cmd,
			Entrypoint: []string{},
			Labels:     map[string]string{dockerutil.CleanupLabel: r.testName},
		},
		NetworkingConfig: &docker.NetworkingConfig{
			EndpointsConfig: map[string]*docker.EndpointConfig{
				r.networkID: {},
			},
		},
		HostConfig: &docker.HostConfig{
			Binds:      r.Bind(),
			AutoRemove: false,
		},
	})
	if err != nil {
		return 1, "", "", err
	}
	if err = r.pool.Client.StartContainerWithContext(cont.ID, nil, ctx); err != nil {
		return 1, "", "", err
	}
	exitCode, err = r.pool.Client.WaitContainerWithContext(cont.ID, ctx)
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)
	_ = r.pool.Client.Logs(docker.LogsOptions{
		Context:      ctx,
		Container:    cont.ID,
		OutputStream: stdoutBuf,
		ErrorStream:  stderrBuf,
		Stdout:       true, Stderr: true,
		Tail: "50", Follow: false,
		Timestamps: false,
	})
	stdout = stdoutBuf.String()
	stderr = stderrBuf.String()
	_ = r.pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: cont.ID, Context: ctx})
	r.log.Debug(
		fmt.Sprintf("stdout:\n%s\nstderr:\n%s", stdout, stderr),
		zap.String("container", container),
	)
	return exitCode, stdout, stderr, err
}

func (r *DockerRelayer) stopContainer() error {
	const timeoutSec = 30 // StopContainer API expects units of whole seconds.
	return r.pool.Client.StopContainer(r.container.ID, timeoutSec)
}

func (r *DockerRelayer) Name() string {
	return r.c.Name() + "-" + dockerutil.SanitizeContainerName(r.testName)
}

// Bind returns the home folder bind point for running the node.
func (r *DockerRelayer) Bind() []string {
	return []string{r.Dir() + ":" + r.NodeHome()}
}

func (r *DockerRelayer) NodeHome() string {
	return "/tmp/relayer-" + r.c.Name()
}

// Dir is the directory where the test node files are stored.
func (r *DockerRelayer) Dir() string {
	return filepath.Join(r.home, r.Name())
}

func (r *DockerRelayer) HostName(pathName string) string {
	return dockerutil.CondenseHostName(fmt.Sprintf("%s-%s", r.c.Name(), pathName))
}

func (r *DockerRelayer) UseDockerNetwork() bool {
	return true
}

type RelayerCommander interface {
	// Name is the name of the relayer, e.g. "rly" or "hermes".
	Name() string

	DefaultContainerImage() string
	DefaultContainerVersion() string

	// ConfigContent generates the content of the config file that will be passed to AddChainConfiguration.
	ConfigContent(ctx context.Context, cfg ibc.ChainConfig, keyName, rpcAddr, grpcAddr string) ([]byte, error)

	// ParseAddKeyOutput processes the output of AddKey
	// to produce the wallet that was created.
	ParseAddKeyOutput(stdout, stderr string) (ibc.RelayerWallet, error)

	// ParseRestoreKeyOutput extracts the address from the output of RestoreKey.
	ParseRestoreKeyOutput(stdout, stderr string) string

	// ParseGetChannelsOutput processes the output of GetChannels
	// to produce the channel output values.
	ParseGetChannelsOutput(stdout, stderr string) ([]ibc.ChannelOutput, error)

	// ParseGetConnectionsOutput processes the output of GetConnections
	// to produce the connection output values.
	ParseGetConnectionsOutput(stdout, stderr string) (ibc.ConnectionOutputs, error)

	// Init is the command to run on the first call to AddChainConfiguration.
	// If the returned command is nil or empty, nothing will be executed.
	Init(homeDir string) []string

	// The remaining methods produce the command to run inside the container.

	AddChainConfiguration(containerFilePath, homeDir string) []string
	AddKey(chainID, keyName, homeDir string) []string
	CreateChannel(pathName string, opts ibc.CreateChannelOptions, homeDir string) []string
	CreateClients(pathName, homeDir string) []string
	CreateConnections(pathName, homeDir string) []string
	FlushAcknowledgements(pathName, channelID, homeDir string) []string
	FlushPackets(pathName, channelID, homeDir string) []string
	GeneratePath(srcChainID, dstChainID, pathName, homeDir string) []string
	GetChannels(chainID, homeDir string) []string
	GetConnections(chainID, homeDir string) []string
	LinkPath(pathName, homeDir string) []string
	RestoreKey(chainID, keyName, mnemonic, homeDir string) []string
	StartRelayer(pathName, homeDir string) []string
	UpdateClients(pathName, homeDir string) []string
}
