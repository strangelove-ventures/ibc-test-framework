package dockerutil

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
)

const (
	ICTDockerPrefix = "interchaintest"
)

var overrideDockerHost string

func init() {
	dockerHost := os.Getenv("DOCKER_HOST")
	if dockerHost != "" {
		ip, err := net.LookupIP(dockerHost)
		if err != nil || len(ip) == 0 {
			overrideDockerHost = dockerHost
		} else {
			overrideDockerHost = ip[0].String()
		}
	}
}

// GetHostPort returns a resource's published port with an address.
// cont is the type returned by the Docker client's ContainerInspect method.
func GetHostPort(cont types.ContainerJSON, portID string) string {
	if cont.NetworkSettings == nil {
		return ""
	}

	p, ok := cont.NetworkSettings.Ports[nat.Port(portID)]
	if !ok {
		// Connect to docker container directly by it's IP address.
		// NOTE: only works if the host is in the same network as the container.
		// Does not work on macOS https://stackoverflow.com/questions/40334508/how-can-i-access-a-docker-container-via-ip-address#answer-40334646
		port := strings.Split(portID, "/")[0]

		// only one network. if there are more than one, we will just return the first one.
		for _, network := range cont.NetworkSettings.Networks {
			return net.JoinHostPort(network.IPAddress, port)
		}

		return ""
	}

	if overrideDockerHost != "" {
		return net.JoinHostPort(overrideDockerHost, p[0].HostPort)
	}

	return net.JoinHostPort(p[0].HostIP, p[0].HostPort)
}

var chars = []byte("abcdefghijklmnopqrstuvwxyz")

// RandLowerCaseLetterString returns a lowercase letter string of given length.
func RandLowerCaseLetterString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func GetDockerUserString() string {
	uid := os.Getuid()
	var usr string
	if runtime.GOOS == "darwin" {
		usr = ""
	} else {
		usr = fmt.Sprintf("%d:%d", uid, uid)
	}
	return usr
}

func GetHeighlinerUserString() string {
	return "1025:1025"
}

func GetRootUserString() string {
	return "0:0"
}

// CondenseHostName truncates the middle of the given name
// if it is 64 characters or longer.
//
// Without this helper, you may see an error like:
//
//	API error (500): failed to create shim: OCI runtime create failed: container_linux.go:380: starting container process caused: process_linux.go:545: container init caused: sethostname: invalid argument: unknown
func CondenseHostName(name string) string {
	if len(name) < 64 {
		return name
	}

	// I wanted to use ... as the middle separator,
	// but that causes resolution problems for other hosts.
	// Instead, use _._ which will be okay if there is a . on either end.
	return name[:30] + "_._" + name[len(name)-30:]
}

var validContainerCharsRE = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

// SanitizeContainerName returns name with any
// invalid characters replaced with underscores.
// Subtests will include slashes, and there may be other
// invalid characters too.
func SanitizeContainerName(name string) string {
	return validContainerCharsRE.ReplaceAllLiteralString(name, "_")
}
