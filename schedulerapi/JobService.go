package schedulerapi

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"

	azblob "github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Microsoft/ApplicationInsights-Go/appinsights"
	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
	errors "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
)

func createScheduler() (*scheduler.Scheduler, error) {
	return scheduler.NewScheduler(*kubeConfig)
}

func getWatcherJobID(jobID string) string {
	return jobID + "-watcher"
}

func getFinalizerJobID(jobID string) string {
	return jobID + "-finalizer"
}

func createWatcherKubernetesJob(k8sScheduler *scheduler.Scheduler, jobID, containerName, blobNamePrefix string, runFinalizerInACI bool) (*batchv1.Job, error) {
	if len(*jobWatcherImage) > 0 && len(*jobFinalizerImage) > 0 {

		watcherJobID := getWatcherJobID(jobID)
		jobDetail := scheduler.AddJobRequest{
			Completions:        1,
			ImageName:          "watcherimage",
			ImagePullSecrets:   *jobImagePullSecret,
			Image:              *jobWatcherImage,
			CPU:                *jobWatcherCPULimit,
			Memory:             *jobWatcherMemoryLimit,
			ServiceAccountName: *jobWatcherServiceAccountName,
			Parallelism:        1,
			JobID:              watcherJobID,
			JobName:            watcherJobID,
			ExecutionLocation:  scheduler.JobExecutionInCluster,
			Annotations: map[string]string{
				"watcher":                       "1",
				StorageContainerAnnotationName:  containerName,
				StorageBlobPrefixAnnotationName: encodeBlobNamePrefix(blobNamePrefix),
			},
			Labels: map[string]string{
				JobCorrelationIDLabelName: jobID,
			},
		}

		jobDetail.AddEnv("JOBIMAGE", *jobFinalizerImage)
		jobDetail.AddEnv("JOBCPULIMIT", *jobFinalizerCPULimit)
		jobDetail.AddEnv("JOBMEMORYLIMIT", *jobFinalizerMemoryLimit)
		jobDetail.AddEnv("JOBIMAGEPULLSECRET", *jobImagePullSecret)
		jobDetail.AddEnv("RUNINACI", strconv.FormatBool(runFinalizerInACI))

		jobDetail.AddEnv(JobStorageAccountNameEnvVarName, *storageAccountName)
		jobDetail.AddEnv(JobStorageAccountKeyEnvVarName, *storageAccountKey)
		jobDetail.AddEnv(JobBlobPrefixEnvVarName, blobNamePrefix)
		jobDetail.AddEnv(JobStorageContainerEnvVarName, containerName)
		jobDetail.AddEnv(JobItemsPerJobEnvVarName, strconv.Itoa(*itemsPerJob))
		jobDetail.AddEnv(JobIDEnvVarName, jobID)
		jobDetail.AddEnv(JobNameEnvVarName, jobID)

		if jobFinishedEventGridSasKey != nil {
			jobDetail.AddEnv(JobEventGridSasKeyEnvVarName, *jobFinishedEventGridSasKey)
		}

		if jobFinishedEventGridTopicEndpoint != nil {
			jobDetail.AddEnv(JobEventGridTopicEndpointEnvVarName, *jobFinishedEventGridTopicEndpoint)
		}

		watcherJob, err := k8sScheduler.NewJob(&jobDetail)
		if watcherJob != nil {
			log.Infof("Started watcher job %s, requiresACI: %t, cpu limit: %s, memory limit: %s, finalizer image: %s", watcherJobID, runFinalizerInACI, *jobWatcherCPULimit, *jobWatcherMemoryLimit, *jobFinalizerImage)
		}

		return watcherJob, err
	}
	return nil, nil
}

func createBatchJob(jobID, containerName, blobNamePrefix string, itemsCount int) (*batchv1.Job, error) {
	k8sScheduler, err := createScheduler()
	if err != nil {
		return nil, err
	}

	completions := int(math.Ceil(float64(itemsCount) / float64(*itemsPerJob)))

	if appInsightsClient != nil {
		event := appinsights.NewEventTelemetry("newJob")
		event.Properties["jobID"] = jobID
		event.Properties["storageContainer"] = containerName
		event.Properties["storagePrefix"] = blobNamePrefix
		event.Properties["image"] = *jobImage
		event.Measurements["items"] = float64(itemsCount)
		event.Properties["cpu"] = *batchCPULimit
		event.Properties["mem"] = *batchMemoryLimit
		event.Properties["aggregatorImage"] = *jobFinalizerImage

		appInsightsClient.Track(event)
	}
	log.Infof("Starting batch job %s, jobs count: %d, completions: %d, cpu: %s, memory: %s", jobID, itemsCount, completions, *batchCPULimit, *batchMemoryLimit)

	jobDetail := scheduler.AddJobRequest{
		Completions:      1,
		Parallelism:      1,
		ImageName:        "batchimage",
		ImagePullSecrets: *jobImagePullSecret,
		Image:            *batchImage,
		CPU:              *batchCPULimit,
		Memory:           *batchMemoryLimit,

		JobID:   jobID,
		JobName: jobID,
		Annotations: map[string]string{
			StorageContainerAnnotationName:  containerName,
			StorageBlobPrefixAnnotationName: encodeBlobNamePrefix(blobNamePrefix),
		},
		Labels: map[string]string{
			JobCorrelationIDLabelName: jobID,
		},
	}

	jobDetail.AddEnv(JobIDEnvVarName, jobID)
	jobDetail.AddEnv("POOLID", *batchPoolID)
	jobDetail.AddEnv("IMAGENAME", *jobImage)
	jobDetail.AddEnv("COMPLETIONS", strconv.Itoa(completions))
	jobDetail.AddEnv("BATCHNAME", *batchName)
	jobDetail.AddEnv("BATCHCONNSTR", *batchConnectionString)
	jobDetail.AddEnv("BATCHPW", *batchPassword)
	jobDetail.AddEnv("AGGIMAGENAME", *jobFinalizerImage)

	jobDetail.AddEnv(JobStorageContainerEnvVarName, containerName)
	jobDetail.AddEnv(JobStorageAccountNameEnvVarName, *storageAccountName)
	jobDetail.AddEnv(JobStorageAccountKeyEnvVarName, *storageAccountKey)
	jobDetail.AddEnv(JobBlobPrefixEnvVarName, blobNamePrefix)

	return k8sScheduler.NewJob(&jobDetail)
}

func createKubernetesJob(jobID, containerName, blobNamePrefix string, locationsCount int, forceAci bool, gpuType string, gpuQuantity int) (*batchv1.Job, error) {

	executionLocation := scheduler.JobExecutionInCluster
	k8sScheduler, err := createScheduler()
	if err != nil {
		return nil, err
	}

	completions := int(math.Ceil(float64(locationsCount) / float64(*itemsPerJob)))
	requiresACI := forceAci || (*aciCompletionsTrigger > 0 && completions >= *aciCompletionsTrigger)
	parallelism := *maxParallelism
	cpuLimit := *jobCPULimit
	memoryLimit := *JobMemoryLimit

	if requiresACI {
		executionLocation = scheduler.JobExecutionInACI
		parallelism = *aciMaxParallelism
		cpuLimit = *aciJobCPULimit
		memoryLimit = *aciJobMemoryLimit
	}

	if parallelism > completions {
		parallelism = completions
	}

	watcherJob, err := createWatcherKubernetesJob(k8sScheduler, jobID, containerName, blobNamePrefix, requiresACI)
	if err != nil {
		return nil, err
	}

	log.Infof("Starting job %s, jobs count: %d, requiresACI: %t, completions: %d, parallelism: %d, cpu limit: %s, memory limit: %s, gpu: %d/%s", jobID, locationsCount, requiresACI, completions, parallelism, cpuLimit, memoryLimit, gpuQuantity, gpuType)

	jobDetail := scheduler.AddJobRequest{
		Completions:       completions,
		Parallelism:       parallelism,
		ImageName:         "jobimage",
		ImagePullSecrets:  *jobImagePullSecret,
		Image:             *jobImage,
		ImageOS:           *jobImageOS,
		CPU:               cpuLimit,
		Memory:            memoryLimit,
		JobID:             jobID,
		JobName:           jobID,
		ExecutionLocation: executionLocation,
		GpuQuantity:       gpuQuantity,
		GpuType:           gpuType,
		Annotations: map[string]string{
			StorageContainerAnnotationName:  containerName,
			StorageBlobPrefixAnnotationName: encodeBlobNamePrefix(blobNamePrefix),
			MainJobAnnotationName:           "1",
			JobHasWatcherAnnotationName:     strconv.FormatBool(watcherJob != nil),
			JobHasFinalizerAnnotationName:   strconv.FormatBool(len(*jobFinalizerImage) > 0),
		},
		Labels: map[string]string{
			JobCorrelationIDLabelName: jobID,
		},
	}

	jobDetail.AddEnv(JobStorageContainerEnvVarName, containerName)
	jobDetail.AddEnv(JobStorageAccountNameEnvVarName, *storageAccountName)
	jobDetail.AddEnv(JobStorageAccountKeyEnvVarName, *storageAccountKey)
	jobDetail.AddEnv(JobBlobPrefixEnvVarName, blobNamePrefix)
	jobDetail.AddEnv(JobItemsPerJobEnvVarName, strconv.Itoa(*itemsPerJob))
	jobDetail.AddEnv(JobIDEnvVarName, jobID)

	k8sJob, err := k8sScheduler.NewJob(&jobDetail)
	if err != nil {
		if watcherJob != nil {
			log.Infof("Worker job failed, deleting watcher job %s", watcherJob.GetName())
			k8sScheduler.DeleteJob(watcherJob.GetName())
		}
	}

	return k8sJob, err
}

// createJobInputFiles creates the input file in blob storage, returning the amount of lines
func createJobInputFiles(r *http.Request, blobContainerName, jobNamePrefix string, splitter InputSplitter) (int, error) {

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

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

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

	return splitter.Split(r.Context(), file, containerURL, jobNamePrefix)
}

func writeAzureBlobs(ctx context.Context, w io.Writer, blobContainerName, blobNamePrefix string) error {
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

	prefix := fmt.Sprintf("%s/results.json", blobNamePrefix)

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

		log.Infof("Found %d blobs to return", len(listBlob.Segment.BlobItems))

		for _, blobItem := range listBlob.Segment.BlobItems {
			log.Infof("Writing %s to response", blobItem.Name)
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
