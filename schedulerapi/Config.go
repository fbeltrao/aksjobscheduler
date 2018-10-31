package schedulerapi

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

// CreatedByLabelName defines the label name for jobs created by this api
const CreatedByLabelName = "created_by"

// CreatedByLabelValue defines the label value for jobs created by this api
const CreatedByLabelValue = "aksscheduler"

// StorageContainerLabelName defines the label name containing the storage container name
const StorageContainerLabelName = "storage_container"

// StorageBlobPrefixLabelName defines the label name containing the storage blob input prefix
const StorageBlobPrefixLabelName = "storage_blob_prefix"

// JobStorageContainerEnvVarName defines the environment variable containing the storage container for the job
const JobStorageContainerEnvVarName = "STORAGECONTAINER"

// JobStorageConnectionStringEnvVarName defines the environment variable containing the storage connection string for the job
const JobStorageConnectionStringEnvVarName = "STORAGECONNECTIONSTRING"

// JobEventGridTopicEndpointEnvVarName defines the environment variable containing the event grid endpoint for the job
const JobEventGridTopicEndpointEnvVarName = "EVENTGRIDTOPICENDPOINT"

// JobEventGridSasKeyEnvVarName defines the environment variable containing the event grid sas key for the job
const JobEventGridSasKeyEnvVarName = "EVENTGRIDSASKEY"

// JobIDEnvVarName defines the environment variable containing the job identifier
const JobIDEnvVarName = "JOBID"

// JobBlobPrefixEnvVarName defines the environment variable containing the storage blob prefix name for the job
const JobBlobPrefixEnvVarName = "BLOBPREFIX"

// JobLinesPerJobEnvVarName defines the environment variable containing the number of files to be processed per job
const JobLinesPerJobEnvVarName = "LINESPERJOB"

// BufferSizeWriteLimit defines the approximated size to write content to a blob (the actual limit is 4MB, we are stopping before)
const BufferSizeWriteLimit int = 1024 * 1024 * 3.5 // 3.5 MB

var (
	// storageAccountName defines the storage account name
	storageAccountName = flag.String("storageAccount", getEnvString("STORAGEACCOUNT", ""), "Storage account name")

	// storageAccountKey defines the storage account key
	storageAccountKey = flag.String("storageKey", getEnvString("STORAGEKEY", ""), "Storage account key")

	// ContainerName defines the Storage container name
	containerName = flag.String("containerName", getEnvString("CONTAINERNAME", "jobs"), "Storage container name ('jobs' by default)")

	// maxParallelism defines the max. amount of concurrent pods in a job when running in local cluster
	maxParallelism = flag.Int("maxParallelism", getEnvInt("MAXPARALLELISM", 2), "Max parallelism for local cluster (2 by default)")

	// aciMaxParallelism defines the max. amount of concurrent pods in a job when running in ACI
	aciMaxParallelism = flag.Int("aciMaxParallelism", getEnvInt("ACIMAXPARALLELISM", 4), "Max parallelism for ACI (4 by default)")

	// linesPerJob defines the amount of lines each job will process
	linesPerJob = flag.Int("linesPerJob", getEnvInt("LINESPERJOB", 100000), "Lines per job (100'000 by default)")

	// aciCompletionsTrigger defines the amount of completions necessary to execute the job using virtual kubelet
	aciCompletionsTrigger = flag.Int("aciCompletionsTrigger", getEnvInt("ACICOMPLETIONSTRIGGER", 6), "Defines the amount of completions necessary to execute the job using virtual kubelet. Default is 6. 0 to disable ACI")

	// aciSelectorHostName defines the ACI host name
	aciSelectorHostName = flag.String("aciSelectorHostName", getEnvString("ACIHOSTNAME", "virtual-kubelet-virtual-kubelet-linux-westeurope"), "Defines the ACI host name ('virtual-kubelet-virtual-kubelet-linux-westeurope' by default)")

	// jobImage defines the image identifier about the job
	jobImage = flag.String("jobImage", getEnvString("JOBIMAGE", "fbeltrao/aksjobscheduler-worker-dotnet:1.0"), "Image to be used in job. Default (fbeltrao/aksjobscheduler-worker-dotnet:1.0)")

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

	if len(*aciSelectorHostName) <= 0 {
		return fmt.Errorf("aci host name has invalid value: %s", *aciSelectorHostName)
	}

	if len(*jobCPULimit) <= 0 {
		return fmt.Errorf("job cpu limit has invalid value: %s", *jobCPULimit)
	}

	if len(*JobMemoryLimit) <= 0 {
		return fmt.Errorf("job memory limit has invalid value: %s", *JobMemoryLimit)
	}

	if len(*jobImage) <= 0 {
		return fmt.Errorf("job image has invalid value: %s", *jobImage)
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

	log.Infof("Storage account: %s\n", *storageAccountName)
	log.Infof("Storage key: %s\n", *storageAccountKey)
	log.Infof("Kube config file: %s\n", *kubeConfig)
	log.Infof("Default container: %s\n", *containerName)
	log.Infof("Max parallelism: %d\n", *maxParallelism)
	log.Infof("ACI max parallelism: %d\n", *aciMaxParallelism)
	log.Infof("Lines per job: %d\n", *linesPerJob)
	log.Infof("Job image: %s\n", *jobImage)
	log.Infof("Job CPU limit: %s\n", *jobCPULimit)
	log.Infof("Job memory limit: %s\n", *JobMemoryLimit)
	log.Infof("ACI job CPU limit: %s\n", *aciJobCPULimit)
	log.Infof("ACI job memory limit: %s\n", *aciJobMemoryLimit)
	log.Infof("ACI file threshold: %d\n", *aciCompletionsTrigger)
	log.Infof("ACI selector host name: %s\n", *aciSelectorHostName)
	log.Infof("Job Finished Event Grid topic endpoint: %s\n", *jobFinishedEventGridTopicEndpoint)
	log.Infof("Job Finished Event Grid sas key: %s\n", *jobFinishedEventGridSasKey)

	return nil
}
