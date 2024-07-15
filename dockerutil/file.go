package dockerutil

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
)

func CopyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func CopyCoverageFromContainer(ctx context.Context, t *testing.T, client *client.Client, containerId string, internalGoCoverDir string, extHostGoCoverDir string) {
	r, _, err := client.CopyFromContainer(ctx, containerId, internalGoCoverDir)
	require.NoError(t, err)
	defer r.Close()

	err = os.MkdirAll(extHostGoCoverDir, os.ModePerm)
	require.NoError(t, err)

	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		require.NoError(t, err)

		var fileBuff bytes.Buffer
		_, err = io.Copy(&fileBuff, tr)
		require.NoError(t, err)

		name := hdr.Name
		extractedFileName := path.Base(name)

		//Only extract coverage files
		if !strings.HasPrefix(extractedFileName, "cov") {
			continue
		}
		isDirectory := extractedFileName == ""
		if isDirectory {
			continue
		}

		filePath := filepath.Join(extHostGoCoverDir, extractedFileName)
		err = os.WriteFile(filePath, fileBuff.Bytes(), os.ModePerm)
		require.NoError(t, err)
	}
}
