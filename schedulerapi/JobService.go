package schedulerapi

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Azure/azure-pipeline-go/pipeline"
	azblob "github.com/Azure/azure-storage-blob-go/azblob"
	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
	errors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func createScheduler() (*scheduler.Scheduler, error) {
	s, err := scheduler.NewScheduler(*kubeConfig)
	if err == nil {
		s.ACISelectorHostName = *aciSelectorHostName
	}
	return s, nil
}

/*
// CreateJobHead produces a new job by creating the required kubernetes job
func CreateJobHead(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var jobCreateRequest JobCreateRequest
	if err := decoder.Decode(&jobCreateRequest); err != nil {
		responseWithApplicationError(w, http.StatusBadRequest, "Invalid payload")
		return
	}

	// verify the job size
	inputFileCount, err := verifyJobInputFiles(r, jobCreateRequest.StorageContainer, jobCreateRequest.StorageBlobPrefix)
	if err != nil {
		responseWithError(w, err)
		return
	}
	err = createJobFromInputFiles(jobCreateRequest.ID, jobCreateRequest.StorageContainer, jobCreateRequest.StorageBlobPrefix, inputFileCount)
	if err != nil {
		responseWithError(w, err)
		return
	}

	w.WriteHeader(202)
	w.Write([]byte(jobCreateRequest.ID))
}
*/

func createJobFromInputLines(jobID, containerName, blobNamePrefix string, linesCount int) error {
	k8sScheduler, err := createScheduler()
	if err != nil {
		return err
	}

	parts := int(math.Ceil(float64(linesCount) / float64(*linesPerJob)))
	completions := parts // completions might be increase to accomodate finished notification
	requiresACI := *aciCompletionsTrigger > 0 && completions >= *aciCompletionsTrigger
	parallelism := *maxParallelism
	cpuLimit := *jobCPULimit
	memoryLimit := *JobMemoryLimit

	finishedNotificationEnabled := len(*jobFinishedEventGridTopicEndpoint) > 0 && len(*jobFinishedEventGridSasKey) > 0

	if finishedNotificationEnabled {
		// add another completion to send the notification
		completions++
	}

	if requiresACI {
		parallelism = *aciMaxParallelism
		cpuLimit = *aciJobCPULimit
		memoryLimit = *aciJobMemoryLimit
	}

	if parallelism > completions {
		parallelism = completions
	}

	// do not parallelize all if the last job will wait
	if finishedNotificationEnabled && parallelism == completions {
		parallelism--
	}

	log.Infof("Starting job %s, lines count: %d, requiresACI: %t, completions: %d, parallelism: %d, cpu limit: %s, memory limit: %s", jobID, linesCount, requiresACI, completions, parallelism, cpuLimit, memoryLimit)

	jobDetail := scheduler.NewJobDetail{
		Completions: completions,
		ImageName:   "jobimage",
		Image:       *jobImage,
		CPU:         cpuLimit,
		Memory:      memoryLimit,
		Parallelism: parallelism,
		JobID:       jobID,
		JobName:     jobID,
		RequiresACI: requiresACI,
		Labels: map[string]string{
			CreatedByLabelName:         CreatedByLabelValue,
			StorageContainerLabelName:  containerName,
			StorageBlobPrefixLabelName: encodeBlobNamePrefix(blobNamePrefix),
			PartsLabelName:             strconv.Itoa(parts),
		},
	}

	jobDetail.AddEnv(JobStorageContainerEnvVarName, containerName)
	jobDetail.AddEnv(JobStorageConnectionStringEnvVarName, fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net", *storageAccountName, *storageAccountKey))
	jobDetail.AddEnv(JobBlobPrefixEnvVarName, blobNamePrefix)
	jobDetail.AddEnv(JobLinesPerJobEnvVarName, strconv.Itoa(*linesPerJob))
	jobDetail.AddEnv(JobIDEnvVarName, jobID)

	if finishedNotificationEnabled {
		jobDetail.AddEnv(JobEventGridTopicEndpointEnvVarName, *jobFinishedEventGridTopicEndpoint)
		jobDetail.AddEnv(JobEventGridSasKeyEnvVarName, *jobFinishedEventGridSasKey)
	}

	_, err = k8sScheduler.NewJob(&jobDetail)

	return err
}

func verifyJobInputFiles(r *http.Request, blobContainerName, jobNamePrefix string) (int, error) {

	inputFileCount := 0

	credential, err := azblob.NewSharedKeyCredential(*storageAccountName, *storageAccountKey)
	if err != nil {
		return inputFileCount, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", *storageAccountName, blobContainerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	listBlobsFlatSegment := azblob.ListBlobsSegmentOptions{
		Prefix: jobNamePrefix,
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(r.Context(), marker, listBlobsFlatSegment)
		if err != nil {
			return 0, err
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		inputFileCount += len(listBlob.Segment.BlobItems)
	}

	return inputFileCount, nil
}

// CreateJobInputFile creates the input file in blob storage, returning the amount of lines
func createJobInputFile(r *http.Request, blobContainerName, jobNamePrefix string) (int, error) {

	log.Debugf("Starting job input file creation. container: %s, prefix: %s", blobContainerName, jobNamePrefix)

	err := r.ParseForm()
	if err != nil {
		return 0, errors.Wrap(err, "error parsing form")
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		return 0, errors.Wrap(err, "error creating form file")
	}

	defer file.Close()

	credential, err := azblob.NewSharedKeyCredential(*storageAccountName, *storageAccountKey)
	if err != nil {
		return 0, err
	}

	// logOptions := pipeline.LogOptions{
	// 	Log: func(level pipeline.LogLevel, message string) {
	// 		log.Printf("[level: %d] %s\n", level, message)
	// 	},
	// 	ShouldLog: func(level pipeline.LogLevel) bool {
	// 		return true
	// 	},
	// }
	logOptions := pipeline.LogOptions{}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Log: logOptions,
	})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", *storageAccountName, blobContainerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	// Create the container
	log.Infof("Creating a container named %s", blobContainerName)
	_, err = containerURL.Create(r.Context(), azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		storageError, _ := err.(azblob.StorageError)
		if storageError == nil || storageError.ServiceCode() != azblob.ServiceCodeContainerAlreadyExists {
			return 0, err
		}
	}

	targetBlobName := fmt.Sprintf("%s/input.json", jobNamePrefix)

	appendBlobURL := containerURL.NewAppendBlobURL(targetBlobName)
	log.Infof("Copying uploaded file to %s", targetBlobName)

	bufferContentSize := 0

	buffer := &bytes.Buffer{}
	totalLines := 0
	currentJobIndex := 1
	currentJobLines := 0
	lastJobByteIndex := 0
	currentByteIndex := 0
	blobCreated := false

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		lineAsByte := scanner.Bytes()

		currentByteIndex += len(lineAsByte)
		totalLines++
		currentJobLines++

		_, err = buffer.Write(lineAsByte)
		if err != nil {
			return 0, err
		}

		newLineSize, err := buffer.WriteRune('\n')
		if err != nil {
			return 0, err
		}
		currentByteIndex += newLineSize

		// create new control file
		if currentJobLines == *linesPerJob {
			controlBlobName := fmt.Sprintf("%s/ctrl_input_%d_%d", jobNamePrefix, currentJobIndex, lastJobByteIndex)
			controlBlobURL := containerURL.NewAppendBlobURL(controlBlobName)

			log.Infof("Creating control blob %s", controlBlobName)
			controlBlobURL.Create(r.Context(),
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
					r.Context(),
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
			_, err := appendBlobURL.AppendBlock(r.Context(), bytes.NewReader(bytesToWrite), azblob.AppendBlobAccessConditions{}, nil)

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
				r.Context(),
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
		_, err := appendBlobURL.AppendBlock(r.Context(), bytes.NewReader(bytesToWrite), azblob.AppendBlobAccessConditions{}, nil)

		if err != nil {
			return 0, err
		}
	}

	if currentJobLines > 0 {
		controlBlobName := fmt.Sprintf("%s/ctrl_input_%d_%d", jobNamePrefix, currentJobIndex, lastJobByteIndex)
		controlBlobURL := containerURL.NewAppendBlobURL(controlBlobName)

		log.Infof("Creating control blob %s", controlBlobName)
		_, err = controlBlobURL.Create(r.Context(),
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

func writeAzureBlobs(ctx context.Context, w io.Writer, blobContainerName, blobNamePrefix string, part int) error {
	credential, err := azblob.NewSharedKeyCredential(*storageAccountName, *storageAccountKey)
	if err != nil {
		return err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", *storageAccountName, blobContainerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	var prefix string
	if part > 0 {
		prefix = fmt.Sprintf("%s/output_%d.", blobNamePrefix, part)
	} else {
		prefix = fmt.Sprintf("%s/output_", blobNamePrefix)
	}

	log.Debugf("Creating results from prefix %s", prefix)
	listBlobsFlatSegment := azblob.ListBlobsSegmentOptions{
		Prefix: prefix,
	}

	const BufferSize = 1024 * 1024 * 4 // 4 MB
	buffer := make([]byte, BufferSize)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, listBlobsFlatSegment)
		if err != nil {
			return err
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		log.Debugf("Found %d blobs to return", len(listBlob.Segment.BlobItems))

		for _, blobItem := range listBlob.Segment.BlobItems {
			log.Debugf("Writing %s to response", blobItem.Name)
			err = streamBlobContentWithBuffer(ctx, w, containerURL.NewBlobURL(blobItem.Name), buffer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func streamBlobContentWithBuffer(ctx context.Context, w io.Writer, blobURL azblob.BlobURL, buffer []byte) error {
	response, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return err
	}

	retryReaderOptions := azblob.RetryReaderOptions{
		MaxRetryRequests: 3,
	}

	body := response.Body(retryReaderOptions)
	defer body.Close()

	_, err = io.CopyBuffer(w, body, buffer)

	return err
}
