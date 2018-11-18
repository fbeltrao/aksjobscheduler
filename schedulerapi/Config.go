package schedulerapi

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	appinsights "github.com/Microsoft/ApplicationInsights-Go/appinsights"
	log "github.com/sirupsen/logrus"
)

// JobCorrelationIDLabelName defines the label name to correlate jobs with additional ones
const JobCorrelationIDLabelName = "job_correlation_id"

// MainJobAnnotationName defines annotation name for main job flag
const MainJobAnnotationName = "job_is_main"

// JobHasWatcherAnnotationName defines if job has a companion watcher job
const JobHasWatcherAnnotationName = "job_has_watcher"

// JobHasFinalizerAnnotationName defines if job has a finalizer
const JobHasFinalizerAnnotationName = "job_has_finalizer"

// StorageContainerAnnotationName defines the label name containing the storage container name
const StorageContainerAnnotationName = "storage_container"

// StorageBlobPrefixAnnotationName defines the label name containing the storage blob input prefix
const StorageBlobPrefixAnnotationName = "storage_blob_prefix"

// JobStorageContainerEnvVarName defines the environment variable containing the storage container for the job
const JobStorageContainerEnvVarName = "CONTAINER"

// JobStorageConnectionStringEnvVarName defines the environment variable containing the storage connection string for the job
const JobStorageConnectionStringEnvVarName = "STORAGECONNECTIONSTRING"

// JobStorageAccountNameEnvVarName defines the environment variable containing the storage account name
const JobStorageAccountNameEnvVarName = "STORAGEACCOUNT"

// JobStorageAccountKeyEnvVarName defines the environment variable containing the storage account key
const JobStorageAccountKeyEnvVarName = "STORAGEKEY"

// JobEventGridTopicEndpointEnvVarName defines the environment variable containing the event grid endpoint for the job
const JobEventGridTopicEndpointEnvVarName = "EVENTGRIDTOPICENDPOINT"

// JobEventGridSasKeyEnvVarName defines the environment variable containing the event grid sas key for the job
const JobEventGridSasKeyEnvVarName = "EVENTGRIDSASKEY"

// JobIDEnvVarName defines the environment variable containing the job identifier
const JobIDEnvVarName = "JOBID"

// JobNameEnvVarName defines the environment variable containing the job name
const JobNameEnvVarName = "JOBNAME"

// JobBlobPrefixEnvVarName defines the environment variable containing the storage blob prefix name for the job
const JobBlobPrefixEnvVarName = "BLOBPREFIX"

// JobItemsPerJobEnvVarName defines the environment variable containing the number of items to be processed per job
const JobItemsPerJobEnvVarName = "ITEMSPERJOB"

// BufferSizeWriteLimit defines the approximated size to write content to a blob (the actual limit is 4MB, we are stopping before)
const BufferSizeWriteLimit int = 1024 * 1024 * 3.5 // 3.5 MB

var (
	appInsightsClient appinsights.TelemetryClient

	// application insights instrumentation key
	instrumentationKey = flag.String("instrumentationKey", getEnvString("INSTRUMENTATION_KEY", ""), "Application Insights instrumentation key")

	// storageAccountName defines the storage account name
	storageAccountName = flag.String("storageAccount", getEnvString(JobStorageAccountNameEnvVarName, ""), "Storage account name")

	// storageAccountKey defines the storage account key
	storageAccountKey = flag.String("storageKey", getEnvString(JobStorageAccountKeyEnvVarName, ""), "Storage account key")

	// ContainerName defines the Storage container name
	containerName = flag.String("containerName", getEnvString("CONTAINERNAME", "jobs"), "Storage container name ('jobs' by default)")

	// maxParallelism defines the max. amount of concurrent pods in a job when running in local cluster
	maxParallelism = flag.Int("maxParallelism", getEnvInt("MAXPARALLELISM", 2), "Max parallelism for local cluster (2 by default)")

	// aciMaxParallelism defines the max. amount of concurrent pods in a job when running in ACI
	aciMaxParallelism = flag.Int("aciMaxParallelism", getEnvInt("ACIMAXPARALLELISM", 50), "Max parallelism for ACI (50 by default)")

	// itemsPerJob defines the amount of items each job will process
	itemsPerJob = flag.Int("itemsPerJob", getEnvInt("ITEMSPERJOB", 100000), "Items per job (100000 by default)")

	// aciCompletionsTrigger defines the amount of completions necessary to execute the job using virtual kubelet
	aciCompletionsTrigger = flag.Int("aciCompletionsTrigger", getEnvInt("ACICOMPLETIONSTRIGGER", 6), "Defines the amount of completions necessary to execute the job using virtual kubelet. Default is 6. 0 to disable ACI")

	// jobImageOS defines the job image OS
	jobImageOS = flag.String("jobImageOS", getEnvString("JOBIMAGEOS", "linux"), "Defines the job image OS (windows, linux). linux by default")

	// jobImage defines the image identifier about the job
	jobImage = flag.String("jobImage", getEnvString("JOBIMAGE", "fbeltrao/aksjobscheduler-worker-dotnet:1.1"), "Image to be used in job. Default (fbeltrao/aksjobscheduler-worker-dotnet:1.0)")

	// JobImagePullSecret defines the image pull secret when using a private image repository
	jobImagePullSecret = flag.String("jobImagePullSecret", getEnvString("JOBIMAGEPULLSECRET", ""), "Defines the image pull secret when using a private image repository")

	// jobCPULimit defines the CPU limit for the job pod when running on local cluster
	jobCPULimit = flag.String("jobCPULimit", getEnvString("JOBCPULIMIT", "0.5"), "Job CPU limit for local cluster (0.5 by default)")

	// aciJobCPULimit defines the CPU limit for the job pod when running on ACI
	aciJobCPULimit = flag.String("aciJobCPULimit", getEnvString("ACIJOBCPULIMIT", "1"), "Job CPU limit for ACI (1 by default)")

	// JobMemoryLimit defines the memory limit for the job pod when running on local cluster
	JobMemoryLimit = flag.String("jobMemoryLimit", getEnvString("JOBMEMORYLIMIT", "256Mi"), "Job Memory limit for local cluster (256Mi by default)")

	// aciJobMemoryLimit defines the memory limit for the job pod when running on ACI
	aciJobMemoryLimit = flag.String("aciJobMemoryLimit", getEnvString("ACIJOBMEMORYLIMIT", "1Gi"), "Job Memory limit for ACI(1Gi by default)")

	// kubeConfig defines the kube config location, only if running outside the cluster
	kubeConfig *string

	// jobFinishedEventGridTopicEndpoint defines event endpoint to publish when job is done
	jobFinishedEventGridTopicEndpoint = flag.String("eventGridTopicEndpoint", getEnvString("EVENTGRIDENDPOINT", ""), "Event Grid event endpoint to publish when job is done")

	// jobFinishedEventGridSasKey defines sas key to publish when job is done
	jobFinishedEventGridSasKey = flag.String("eventGridSasKey", getEnvString("EVENTGRIDSASKEY", ""), "Event Grid sas key publish when job is done")

	// jobWatcherImage defines image for watcher
	jobWatcherImage = flag.String("jobWatcherImage", getEnvString("JOBWATCHERIMAGE", "fbeltrao/aksjobscheduler-watcher:1.0"), "Job watch image (default fbeltrao/aksjobscheduler-watcher:1.0)")

	// jobWatcherServiceAccountName defines the service account name used by the job watcher
	jobWatcherServiceAccountName = flag.String("jobWatcherServiceAccountName", getEnvString("JOBWATCHERSERVICEACCOUNTNAME", ""), "Job watch image")

	// jobWatcherCPULimit defines the CPU limit for the job pod when running on local cluster
	jobWatcherCPULimit = flag.String("jobWatcherCPULimit", getEnvString("JOBWATCHERCPULIMIT", "0.1"), "Job CPU limit for local cluster (0.1 by default)")

	// jobWatcherMemoryLimit defines the memory limit for the job pod when running on local cluster
	jobWatcherMemoryLimit = flag.String("jobWatcherMemoryLimit", getEnvString("JOBWATCHERMEMORYLIMIT", "80Mi"), "Job Memory limit for local cluster (80Mi by default)")

	// jobWatcherImage defines image for watcher
	jobFinalizerImage = flag.String("jobFinalizerImage", getEnvString("JOBFINALIZERIMAGE", "fbeltrao/aksjobscheduler-finalizer-dotnet:1.0"), "Job finalizer image (by default fbeltrao/aksjobscheduler-finalizer-dotnet:1.0)")

	// jobFinalizerCPULimit defines the CPU limit for the job pod when running on local cluster
	jobFinalizerCPULimit = flag.String("jobFinalizerCPULimit", getEnvString("JOBFINALIZERCPULIMIT", "0.5"), "Job CPU limit for local cluster (0.5 by default)")

	// jobFinalizerMemoryLimit defines the memory limit for the job pod when running on local cluster
	jobFinalizerMemoryLimit = flag.String("jobFinalizerMemoryLimit", getEnvString("JOBFINALIZERMEMORYLIMIT", "256Mi"), "Job Memory limit for local cluster (256Mi by default)")

	batchImage            = flag.String("batchImage", getEnvString("BATCHIMAGE", ""), "Batch image")
	batchName             = flag.String("batchName", getEnvString("BATCHNAME", ""), "Batch name")
	batchConnectionString = flag.String("batchConnectionString", getEnvString("BATCHCONNECTIONSTRING", ""), "Batch connection string")
	batchPassword         = flag.String("batchPassword", getEnvString("BATCHPASSWORD", ""), "Batch password")
	batchPoolID           = flag.String("batchPoolID", getEnvString("BATCHPOOLID", ""), "Batch pool id")
	batchCPULimit         = flag.String("batchCPULimit", getEnvString("BATCHCPULIMIT", "0.5"), "Batch CPU limit (default 0.5)")
	batchMemoryLimit      = flag.String("batchMemoryLimit", getEnvString("BATCHMEMORYLIMIT", "500Mi"), "Batch memory limit")
)

// Initialize configures the api
func Initialize() error {

	// setup log to console
	debugLogging := getEnvBool("DEBUGLOG", false)
	initLogging(debugLogging)

	if home := homeDir(); home != "" {
		kubeConfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeConfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	if len(*jobCPULimit) <= 0 {
		return fmt.Errorf("job cpu limit has invalid value: %s", *jobCPULimit)
	}

	if len(*JobMemoryLimit) <= 0 {
		return fmt.Errorf("job memory limit has invalid value: %s", *JobMemoryLimit)
	}

	if len(*jobImage) <= 0 {
		return fmt.Errorf("job image has invalid value: %s", *jobImage)
	}

	if len(*jobImageOS) <= 0 {
		return fmt.Errorf("job image OS has invalid value: %s", *jobImageOS)
	}

	if *maxParallelism <= 0 {
		return fmt.Errorf("max parallelism has invalid value: %d", *maxParallelism)
	}

	if *aciMaxParallelism <= 0 {
		return fmt.Errorf("aci max parallelism has invalid value: %d", *aciMaxParallelism)
	}

	if len(*storageAccountName) == 0 {
		return errors.New("could not find storage account name value")
	}

	if len(*storageAccountKey) == 0 {
		return errors.New("could not find storage account key value")
	}

	if len(*kubeConfig) > 0 {
		_, err := os.Stat(*kubeConfig)
		if err != nil {
			*kubeConfig = ""
		}
	}

	log.Infof("Application Insights: %s", *instrumentationKey)
	log.Infof("Storage account: %s", *storageAccountName)
	log.Infof("Storage key: %s", *storageAccountKey)
	log.Infof("Kube config file: %s", *kubeConfig)
	log.Infof("Container name: %s", *containerName)
	log.Infof("Max parallelism: %d", *maxParallelism)
	log.Infof("ACI max parallelism: %d", *aciMaxParallelism)
	log.Infof("Items per job: %d", *itemsPerJob)
	log.Infof("Job Image: %s", *jobImage)
	log.Infof("Job Image OS: %s", *jobImageOS)
	log.Infof("Job Image Pull Secret: %s", *jobImagePullSecret)
	log.Infof("Job CPU limit: %s", *jobCPULimit)
	log.Infof("Job Memory limit: %s", *JobMemoryLimit)
	log.Infof("ACI Job CPU limit: %s", *aciJobCPULimit)
	log.Infof("ACI Job memory limit: %s", *aciJobMemoryLimit)
	log.Infof("ACI File threshold: %d", *aciCompletionsTrigger)
	log.Infof("Job Finished Event Grid topic endpoint: %s", *jobFinishedEventGridTopicEndpoint)
	log.Infof("Job Finished Event Grid sas key: %s", *jobFinishedEventGridSasKey)

	log.Infof("Job Watcher Image: %s", *jobWatcherImage)
	log.Infof("Job Watcher Service Account Name: %s", *jobWatcherServiceAccountName)
	log.Infof("Job Watcher CPU limit: %s", *jobWatcherCPULimit)
	log.Infof("Job Watcher Memory limit: %s", *jobWatcherMemoryLimit)

	log.Infof("Job Finalizer Image: %s", *jobFinalizerImage)
	log.Infof("Job Finalizer CPU limit: %s", *jobFinalizerCPULimit)
	log.Infof("Job Finalizer Memory limit: %s", *jobFinalizerMemoryLimit)

	log.Infof("Batch Image: %s", *batchImage)
	log.Infof("Batch Name: %s", *batchName)
	log.Infof("Batch CPU limit: %s", *batchCPULimit)
	log.Infof("Batch Memory limit: %s", *batchMemoryLimit)
	log.Infof("Batch Connection string: %s", *batchConnectionString)
	log.Infof("Batch Password: %s", *batchPassword)
	log.Infof("Batch Pool id: %s", *batchPoolID)

	if len(*instrumentationKey) > 0 {
		appInsightsClient = appinsights.NewTelemetryClient(*instrumentationKey)
	}

	return nil
}
