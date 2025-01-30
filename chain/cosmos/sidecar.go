package cosmos

import (
	"context"
	"fmt"
	"os"

	dockerclient "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"go.uber.org/zap"

	"github.com/strangelove-ventures/interchaintest/v8/dockerutil"
	"github.com/strangelove-ventures/interchaintest/v8/ibc"
)

type SidecarProcesses []*SidecarProcess

// SidecarProcess represents a companion process that may be required on a per chain or per validator basis.
type SidecarProcess struct {
	log *zap.Logger

	Index int
	Chain ibc.Chain

	// If true this process is scoped to a specific validator, otherwise it is scoped at the chain level.
	validatorProcess bool

	// If true this process should be started before the chain or validator, otherwise it should be explicitly started after.
	preStart   bool
	startCheck func(int) error

	ProcessName string
	TestName    string

	VolumeName   string
	DockerClient *dockerclient.Client
	NetworkID    string
	Image        ibc.DockerImage
	ports        nat.PortMap
	startCmd     []string
	env          []string
	homeDir      string

	containerLifecycle *dockerutil.ContainerLifecycle
}

// NewSidecar instantiates a new SidecarProcess.
func NewSidecar(
	log *zap.Logger,
	validatorProcess bool,
	preStart bool,
	startCheck func(int) error,
	chain ibc.Chain,
	dockerClient *dockerclient.Client,
	networkID, processName, testName string,
	image ibc.DockerImage,
	homeDir string,
	index int,
	ports []string,
	startCmd []string,
	env []string,
) *SidecarProcess {
	processPorts := nat.PortMap{}

	for _, port := range ports {
		processPorts[nat.Port(port)] = []nat.PortBinding{}
	}

	if homeDir == "" {
		homeDir = "/home/sidecar"
	}

	s := &SidecarProcess{
		log:              log,
		Index:            index,
		Chain:            chain,
		preStart:         preStart,
		startCheck:       startCheck,
		validatorProcess: validatorProcess,
		ProcessName:      processName,
		TestName:         testName,
		DockerClient:     dockerClient,
		NetworkID:        networkID,
		Image:            image,
		homeDir:          homeDir,
		ports:            processPorts,
		startCmd:         startCmd,
		env:              env,
	}
	s.containerLifecycle = dockerutil.NewContainerLifecycle(log, dockerClient, s.Name())

	return s
}

// Name returns a string identifier based on if this process is configured to run on a chain level or
// on a per validator level.
func (s *SidecarProcess) Name() string {
	if s.validatorProcess {
		return fmt.Sprintf("%s-%s-val-%d-%s", s.Chain.Config().ChainID, s.ProcessName, s.Index, dockerutil.SanitizeContainerName(s.TestName))
	}

	return fmt.Sprintf("%s-%s-%d-%s", s.Chain.Config().ChainID, s.ProcessName, s.Index, dockerutil.SanitizeContainerName(s.TestName))
}

func (s *SidecarProcess) logger() *zap.Logger {
	return s.log.With(
		zap.String("process_name", s.ProcessName),
		zap.String("test", s.TestName),
	)
}

func (s *SidecarProcess) CreateContainer(ctx context.Context) error {
	return s.containerLifecycle.CreateContainer(ctx, s.TestName, s.NetworkID, s.Image, s.ports, "", s.Bind(), nil, s.HostName(), s.startCmd, s.env, []string{})
}

func (s *SidecarProcess) StartContainer(ctx context.Context) error {
	return s.containerLifecycle.StartContainer(ctx)
}

func (s *SidecarProcess) PauseContainer(ctx context.Context) error {
	return s.containerLifecycle.PauseContainer(ctx)
}

func (s *SidecarProcess) UnpauseContainer(ctx context.Context) error {
	return s.containerLifecycle.UnpauseContainer(ctx)
}

func (s *SidecarProcess) StopContainer(ctx context.Context) error {
	return s.containerLifecycle.StopContainer(ctx)
}

func (s *SidecarProcess) RemoveContainer(ctx context.Context) error {
	return s.containerLifecycle.RemoveContainer(ctx)
}

// Bind returns the home folder bind point for running the process.
func (s *SidecarProcess) Bind() []string {
	return []string{fmt.Sprintf("%s:%s", s.VolumeName, s.HomeDir())}
}

// HomeDir returns the path name where any configuration files will be written to the Docker filesystem.
func (s *SidecarProcess) HomeDir() string {
	return s.homeDir
}

func (s *SidecarProcess) HostName() string {
	return dockerutil.CondenseHostName(s.Name())
}

func (s *SidecarProcess) GetHostPorts(ctx context.Context, portIDs ...string) ([]string, error) {
	return s.containerLifecycle.GetHostPorts(ctx, portIDs...)
}

// WriteFile accepts file contents in a byte slice and writes the contents to
// the docker filesystem. relPath describes the location of the file in the
// docker volume relative to the home directory.
func (s *SidecarProcess) WriteFile(ctx context.Context, content []byte, relPath string) error {
	fw := dockerutil.NewFileWriter(s.logger(), s.DockerClient, s.TestName)
	return fw.WriteFile(ctx, s.VolumeName, relPath, content)
}

// CopyFile adds a file from the host filesystem to the docker filesystem
// relPath describes the location of the file in the docker volume relative to
// the home directory.
func (s *SidecarProcess) CopyFile(ctx context.Context, srcPath, dstPath string) error {
	content, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}
	return s.WriteFile(ctx, content, dstPath)
}

// ReadFile reads the contents of a single file at the specified path in the docker filesystem.
// relPath describes the location of the file in the docker volume relative to the home directory.
func (s *SidecarProcess) ReadFile(ctx context.Context, relPath string) ([]byte, error) {
	fr := dockerutil.NewFileRetriever(s.logger(), s.DockerClient, s.TestName)
	gen, err := fr.SingleFileContent(ctx, s.VolumeName, relPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file at %s: %w", relPath, err)
	}
	return gen, nil
}

// Exec enables the execution of arbitrary CLI cmds against the process.
func (s *SidecarProcess) Exec(ctx context.Context, cmd []string, env []string) ([]byte, []byte, error) {
	job := dockerutil.NewImage(s.logger(), s.DockerClient, s.NetworkID, s.TestName, s.Image.Repository, s.Image.Version)
	opts := dockerutil.ContainerOptions{
		Env:   env,
		Binds: s.Bind(),
	}
	res := job.Run(ctx, cmd, opts)
	return res.Stdout, res.Stderr, res.Err
}
