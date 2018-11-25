package schedulerapi

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"

	azblob "github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
)

// InputSplitterByLine splits a big file in lines
type InputSplitterByLine struct {
	linesPerFile int
}

// NewInputSplitterByLine creates a new instance of InputSplitterByLine
func NewInputSplitterByLine(linesPerFile int) InputSplitterByLine {
	return InputSplitterByLine{
		linesPerFile,
	}
}

// Split create smaller files from a big calculation input
func (splitter InputSplitterByLine) Split(ctx context.Context, reader io.Reader, containerURL azblob.ContainerURL, jobNamePrefix string) (int, error) {

	targetBlobName := fmt.Sprintf("%s/input.json", jobNamePrefix)
	appendBlobURL := containerURL.NewAppendBlobURL(targetBlobName)

	log.Infof("Starting file split by line. Copying uploaded file to %s", targetBlobName)

	bufferContentSize := 0

	buffer := &bytes.Buffer{}
	totalLines := 0
	currentJobIndex := 1
	currentJobLines := 0
	lastJobByteIndex := 0
	currentByteIndex := 0
	blobCreated := false

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {

		lineAsByte := scanner.Bytes()

		currentByteIndex += len(lineAsByte)
		totalLines++
		currentJobLines++

		_, err := buffer.Write(lineAsByte)
		if err != nil {
			return 0, err
		}

		newLineSize, err := buffer.WriteRune('\n')
		if err != nil {
			return 0, err
		}
		currentByteIndex += newLineSize

		// create new control file
		if currentJobLines == splitter.linesPerFile {
			controlBlobName := fmt.Sprintf("%s/ctrl_input_%d_%d", jobNamePrefix, currentJobIndex, lastJobByteIndex)
			controlBlobURL := containerURL.NewAppendBlobURL(controlBlobName)

			log.Infof("Creating control blob %s", controlBlobName)
			controlBlobURL.Create(ctx,
				azblob.BlobHTTPHeaders{},
				azblob.Metadata{
					"control": strconv.Itoa(currentJobIndex),
				},
				azblob.BlobAccessConditions{})

			if err != nil {
				return 0, err
			}

			lastJobByteIndex = currentByteIndex
			currentJobIndex++
			currentJobLines = 0
		}

		bufferContentSize += len(lineAsByte) + newLineSize

		if bufferContentSize >= BufferSizeWriteLimit {
			if !blobCreated {
				_, err := appendBlobURL.Create(
					ctx,
					azblob.BlobHTTPHeaders{
						ContentType: "application/json",
					},
					azblob.Metadata{},
					azblob.BlobAccessConditions{},
				)
				if err != nil {
					return 0, err
				}
				blobCreated = true
			}
			bytesToWrite := buffer.Bytes()
			log.Debugf("Appending %d bytes to %s", len(bytesToWrite), targetBlobName)
			_, err := appendBlobURL.AppendBlock(ctx, bytes.NewReader(bytesToWrite), azblob.AppendBlobAccessConditions{}, nil)

			if err != nil {
				return 0, err
			}

			bufferContentSize = 0
			buffer.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	if bufferContentSize > 0 {
		if !blobCreated {
			_, err := appendBlobURL.Create(
				ctx,
				azblob.BlobHTTPHeaders{
					ContentType: "application/json",
				},
				azblob.Metadata{},
				azblob.BlobAccessConditions{},
			)
			if err != nil {
				return 0, err
			}
			blobCreated = true
		}
		bytesToWrite := buffer.Bytes()
		log.Debugf("Appending %d bytes to %s", len(bytesToWrite), targetBlobName)
		_, err := appendBlobURL.AppendBlock(ctx, bytes.NewReader(bytesToWrite), azblob.AppendBlobAccessConditions{}, nil)

		if err != nil {
			return 0, err
		}
	}

	if currentJobLines > 0 {
		controlBlobName := fmt.Sprintf("%s/ctrl_input_%d_%d", jobNamePrefix, currentJobIndex, lastJobByteIndex)
		controlBlobURL := containerURL.NewAppendBlobURL(controlBlobName)

		log.Infof("Creating control blob %s", controlBlobName)
		_, err := controlBlobURL.Create(ctx,
			azblob.BlobHTTPHeaders{},
			azblob.Metadata{
				"control": strconv.Itoa(currentJobIndex),
			},
			azblob.BlobAccessConditions{})

		if err != nil {
			return 0, err
		}
	}

	return totalLines, nil
}
