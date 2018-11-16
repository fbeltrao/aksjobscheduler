package schedulerapi

import (
	"context"
	"io"

	azblob "github.com/Azure/azure-storage-blob-go/azblob"
)

// InputSplitter defines a splitting input file type
type InputSplitter interface {
	Split(ctx context.Context, reader io.Reader, containerURL azblob.ContainerURL, jobNamePrefix string) (int, error)
}
