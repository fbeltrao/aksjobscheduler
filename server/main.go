package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	azblob "github.com/Azure/azure-storage-blob-go/azblob"
	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
	"github.com/gorilla/mux"
	batchv1 "k8s.io/api/batch/v1"
)

// Job defines a working item
type Job struct {
	ID                string     `json:"id"`
	Status            string     `json:"status"`
	StartTime         *time.Time `json:"startTime,omitempty"`
	CompletionTime    *time.Time `json:"completionTime,omitempty"`
	Active            int        `json:"active,omitempty"`
	Succeeded         int        `json:"succeeded,omitempty"`
	Failed            int        `json:"failed,omitempty"`
	Parallelism       int        `json:"parallelism,omitempty"`
	Completions       int        `json:"completions,omitempty"`
	StorageContainer  string     `json:"storageContainer,omitempty"`
	StorageBlobPrefix string     `json:"storageBlobPrefix,omitempty"`
	RunningOnACI      bool       `json:"runningOnAci"`
}

// JobCreateRequest defines a request to create a job where the files already exist
type JobCreateRequest struct {
	ID                string `json:"id"`
	StorageContainer  string `json:"storageContainer,omitempty"`
	StorageBlobPrefix string `json:"storageBlobPrefix,omitempty"`
}

// Globals

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
	// StorageAccountName defines the storage account name
	StorageAccountName = flag.String("storageAccount", getEnvString("STORAGEACCOUNT", ""), "Storage account name")

	// StorageAccountKey defines the storage account key
	StorageAccountKey = flag.String("storageKey", getEnvString("STORAGEKEY", ""), "Storage account key")

	// ContainerName defines the Storage container name
	ContainerName = flag.String("containerName", getEnvString("CONTAINERNAME", "jobs"), "Storage container name ('jobs' by default)")

	// MaxParallelism defines the max. amount of concurrent pods in a job when running in local cluster
	MaxParallelism = flag.Int("maxParallelism", getEnvInt("MAXPARALLELISM", 2), "Max parallelism for local cluster (2 by default)")

	// ACIMaxParallelism defines the max. amount of concurrent pods in a job when running in ACI
	ACIMaxParallelism = flag.Int("aciMaxParallelism", getEnvInt("ACIMAXPARALLELISM", 4), "Max parallelism for ACI (4 by default)")

	// LinesPerJob defines the amount of lines each job will process
	LinesPerJob = flag.Int("linesPerJob", getEnvInt("LINESPERJOB", 100000), "Lines per job (100'000 by default)")

	// ACICompletionsTrigger defines the amount of completions necessary to execute the job using virtual kubelet
	ACICompletionsTrigger = flag.Int("aciCompletionsTrigger", getEnvInt("ACICOMPLETIONSTRIGGER", 6), "Defines the amount of completions necessary to execute the job using virtual kubelet. Default is 6. 0 to disable ACI")

	// ACISelectorHostName defines the ACI host name
	ACISelectorHostName = flag.String("aciSelectorHostName", getEnvString("ACIHOSTNAME", "virtual-kubelet-virtual-kubelet-linux-westeurope"), "Defines the ACI host name ('virtual-kubelet-virtual-kubelet-linux-westeurope' by default)")

	// JobImage defines the image identifier about the job
	JobImage = flag.String("jobImage", getEnvString("JOBIMAGE", "fbeltrao/aksjobscheduler-worker-dotnet:1.0"), "Image to be used in job. Default (fbeltrao/aksjobscheduler-worker-dotnet:1.0)")

	// JobCPULimit defines the CPU limit for the job pod when running on local cluster
	JobCPULimit = flag.String("jobCPULimit", getEnvString("JOBCPULIMIT", "0.5"), "Job CPU limit for local cluster (0.5 by default)")

	// ACIJobCPULimit defines the CPU limit for the job pod when running on ACI
	ACIJobCPULimit = flag.String("aciJobCPULimit", getEnvString("ACIJOBCPULIMIT", "1"), "Job CPU limit for ACI (1 by default)")

	// JobMemoryLimit defines the memory limit for the job pod when running on local cluster
	JobMemoryLimit = flag.String("jobMemoryLimit", getEnvString("JOBMEMORYLIMIT", "256Mi"), "Job Memory limit for local cluster (256Mi by default)")

	// ACIJobMemoryLimit defines the memory limit for the job pod when running on ACI
	ACIJobMemoryLimit = flag.String("aciJobMemoryLimit", getEnvString("ACIJOBMEMORYLIMIT", "1Gi"), "Job Memory limit for ACI(1Gi by default)")

	// KubeConfig defines the kube config location, only if running outside the cluster
	KubeConfig *string

	// JobFinishedEventGridTopicEndpoint defines event endpoint to publish when job is done
	JobFinishedEventGridTopicEndpoint = flag.String("eventGridTopicEndpoint", getEnvString("EVENTGRIDENDPOINT", ""), "Event Grid event endpoint to publish when job is done")

	// JobFinishedEventGridSasKey defines sas key to publish when job is done
	JobFinishedEventGridSasKey = flag.String("eventGridSasKey", getEnvString("EVENTGRIDSASKEY", ""), "Event Grid sas key publish when job is done")
)

func main() {

	err := initialize()
	if err != nil {
		log.Fatal(err.Error())
	}

	router := mux.NewRouter()
	//router.HandleFunc("/jobs", CreateJobHead).Methods("POST").Headers("Content-Type", "application/json")
	router.HandleFunc("/jobs", CreateJob).Methods("POST")
	router.HandleFunc("/jobs", GetJobList).Methods("GET")
	router.HandleFunc("/jobs/{id}", GetJob).Methods("GET")
	router.HandleFunc("/jobs/{id}", DeleteJob).Methods("DELETE")
	log.Println("Listening on :8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func getEnvString(key, defaultValue string) string {
	result := os.Getenv(key)
	if len(result) > 0 {
		return result
	}

	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	envValue := os.Getenv(key)
	if len(envValue) > 0 {
		parsed, err := strconv.ParseBool(envValue)
		if err == nil {
			return parsed
		}

	}

	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	envValue := os.Getenv(key)
	if len(envValue) > 0 {
		parsed, err := strconv.Atoi(envValue)
		if err == nil {
			return parsed
		}
	}

	return defaultValue
}

func initialize() error {

	// setup log to console
	log.SetOutput(os.Stdout)

	if home := homeDir(); home != "" {
		KubeConfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		KubeConfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	if len(*ACISelectorHostName) <= 0 {
		return fmt.Errorf("aci host name has invalid value: %s", *ACISelectorHostName)
	}

	if len(*JobCPULimit) <= 0 {
		return fmt.Errorf("job cpu limit has invalid value: %s", *JobCPULimit)
	}

	if len(*JobMemoryLimit) <= 0 {
		return fmt.Errorf("job memory limit has invalid value: %s", *JobMemoryLimit)
	}

	if len(*JobImage) <= 0 {
		return fmt.Errorf("job image has invalid value: %s", *JobImage)
	}

	if *MaxParallelism <= 0 {
		return fmt.Errorf("max parallelism has invalid value: %d", *MaxParallelism)
	}

	if *ACIMaxParallelism <= 0 {
		return fmt.Errorf("aci max parallelism has invalid value: %d", *ACIMaxParallelism)
	}

	if len(*StorageAccountName) == 0 {
		return errors.New("could not find storage account name value")
	}

	if len(*StorageAccountKey) == 0 {
		return errors.New("could not find storage account key value")
	}

	if len(*KubeConfig) > 0 {
		_, err := os.Stat(*KubeConfig)
		if err != nil {
			*KubeConfig = ""
		}
	}

	log.Printf("Storage account: %s\n", *StorageAccountName)
	log.Printf("Storage key: %s\n", *StorageAccountKey)
	log.Printf("Kube config file: %s\n", *KubeConfig)
	log.Printf("Default container: %s\n", *ContainerName)
	log.Printf("Max parallelism: %d\n", *MaxParallelism)
	log.Printf("ACI max parallelism: %d\n", *ACIMaxParallelism)
	log.Printf("Lines per job: %d\n", *LinesPerJob)
	log.Printf("Job image: %s\n", *JobImage)
	log.Printf("Job CPU limit: %s\n", *JobCPULimit)
	log.Printf("Job memory limit: %s\n", *JobMemoryLimit)
	log.Printf("ACI job CPU limit: %s\n", *ACIJobCPULimit)
	log.Printf("ACI job memory limit: %s\n", *ACIJobMemoryLimit)
	log.Printf("ACI file threshold: %d\n", *ACICompletionsTrigger)
	log.Printf("ACI selector host name: %s\n", *ACISelectorHostName)
	log.Printf("Job Finished Event Grid topic endpoint: %s\n", *JobFinishedEventGridTopicEndpoint)
	log.Printf("Job Finished Event Grid sas key: %s\n", *JobFinishedEventGridSasKey)

	return nil
}

func createScheduler() (*scheduler.Scheduler, error) {
	s, err := scheduler.NewScheduler(*KubeConfig)
	if err == nil {
		s.ACISelectorHostName = *ACISelectorHostName
	}
	return s, nil
}

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
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

// CreateJob produces a new job by splitting the uploaded file
func CreateJob(w http.ResponseWriter, r *http.Request) {

	log.Printf("Received request to create job, content-size: %d, content-type is %s\n", r.ContentLength, r.Header.Get("Content-Type"))

	date := time.Now()
	jobUniqueID := randomString()
	jobID := fmt.Sprintf("%s-%s", date.Format("2006-01"), jobUniqueID)
	blobNamePrefix := fmt.Sprintf("%s/%s", date.Format("2006-01"), jobUniqueID)

	linesCount, err := createJobInputFile(r, *ContainerName, blobNamePrefix)
	if err != nil {
		responseWithError(w, err)
		return
	}

	// start job based on input and amount of files created
	log.Printf("Finished creating file, found %d lines", linesCount)

	err = createJobFromInputLines(jobID, *ContainerName, blobNamePrefix, linesCount)
	if err != nil {
		responseWithError(w, err)
		return
	}

	w.WriteHeader(202)
	w.Write([]byte(jobID))
}

func createJobFromInputLines(jobID, containerName, blobNamePrefix string, linesCount int) error {
	k8sScheduler, err := createScheduler()
	if err != nil {
		return err
	}

	completions := int(math.Ceil(float64(linesCount) / float64(*LinesPerJob)))
	requiresACI := *ACICompletionsTrigger > 0 && completions >= *ACICompletionsTrigger
	parallelism := *MaxParallelism
	cpuLimit := *JobCPULimit
	memoryLimit := *JobMemoryLimit

	finishedNotificationEnabled := len(*JobFinishedEventGridTopicEndpoint) > 0 && len(*JobFinishedEventGridSasKey) > 0

	if finishedNotificationEnabled {
		// add another completion to send the notification
		completions++
	}

	if requiresACI {
		parallelism = *ACIMaxParallelism
		cpuLimit = *ACIJobCPULimit
		memoryLimit = *ACIJobMemoryLimit
	}

	if parallelism > completions {
		parallelism = completions
	}

	// do not parallelize all if the last job will wait
	if finishedNotificationEnabled && parallelism == completions {
		parallelism--
	}

	log.Printf("Starting job %s, lines count: %d, requiresACI: %t, completions: %d, parallelism: %d, cpu limit: %s, memory limit: %s", jobID, linesCount, requiresACI, completions, parallelism, cpuLimit, memoryLimit)

	jobDetail := scheduler.NewJobDetail{
		Completions: completions,
		ImageName:   "jobimage",
		Image:       *JobImage,
		CPU:         cpuLimit,
		Memory:      memoryLimit,
		Parallelism: parallelism,
		JobID:       jobID,
		JobName:     jobID,
		RequiresACI: requiresACI,
		Labels: map[string]string{
			CreatedByLabelName:         CreatedByLabelValue,
			StorageContainerLabelName:  containerName,
			StorageBlobPrefixLabelName: base64.StdEncoding.EncodeToString([]byte(blobNamePrefix)),
		},
	}

	jobDetail.AddEnv(JobStorageContainerEnvVarName, containerName)
	jobDetail.AddEnv(JobStorageConnectionStringEnvVarName, fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net", *StorageAccountName, *StorageAccountKey))
	jobDetail.AddEnv(JobBlobPrefixEnvVarName, blobNamePrefix)
	jobDetail.AddEnv(JobLinesPerJobEnvVarName, strconv.Itoa(*LinesPerJob))
	jobDetail.AddEnv(JobIDEnvVarName, jobID)

	if finishedNotificationEnabled {
		jobDetail.AddEnv(JobEventGridTopicEndpointEnvVarName, *JobFinishedEventGridTopicEndpoint)
		jobDetail.AddEnv(JobEventGridSasKeyEnvVarName, *JobFinishedEventGridSasKey)
	}

	_, err = k8sScheduler.NewJob(&jobDetail)

	return err
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func verifyJobInputFiles(r *http.Request, containerName, jobNamePrefix string) (int, error) {

	inputFileCount := 0

	credential, err := azblob.NewSharedKeyCredential(*StorageAccountName, *StorageAccountKey)
	if err != nil {
		return inputFileCount, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", *StorageAccountName, *ContainerName))

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
func createJobInputFile(r *http.Request, containerName, jobNamePrefix string) (int, error) {
	err := r.ParseForm()
	if err != nil {
		return 0, err
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		return 0, err
	}

	defer file.Close()

	credential, err := azblob.NewSharedKeyCredential(*StorageAccountName, *StorageAccountKey)
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
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", *StorageAccountName, *ContainerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	// Create the container
	log.Printf("Creating a container named %s\n", containerName)
	_, err = containerURL.Create(r.Context(), azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		storageError, _ := err.(azblob.StorageError)
		if storageError == nil || storageError.ServiceCode() != azblob.ServiceCodeContainerAlreadyExists {
			return 0, err
		}
	}

	targetBlobName := fmt.Sprintf("%s/input.json", jobNamePrefix)

	appendBlobURL := containerURL.NewAppendBlobURL(targetBlobName)
	log.Printf("Copying uploaded file to %s", targetBlobName)

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
		if currentJobLines == *LinesPerJob {
			controlBlobName := fmt.Sprintf("%s/ctrl_input_%d_%d", jobNamePrefix, currentJobIndex, lastJobByteIndex)
			controlBlobURL := containerURL.NewAppendBlobURL(controlBlobName)

			log.Printf("Creating control blob %s\n", controlBlobName)
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
		_, err := appendBlobURL.AppendBlock(r.Context(), bytes.NewReader(buffer.Bytes()), azblob.AppendBlobAccessConditions{}, nil)

		if err != nil {
			return 0, err
		}
	}

	if currentJobLines > 0 {
		controlBlobName := fmt.Sprintf("%s/ctrl_input_%d_%d", jobNamePrefix, currentJobIndex, lastJobByteIndex)
		controlBlobURL := containerURL.NewAppendBlobURL(controlBlobName)

		log.Printf("Creating control blob %s\n", controlBlobName)
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

func responseWithError(w http.ResponseWriter, err error) {
	errorMsg := err.Error()
	log.Println(errorMsg)

	respondWithJSON(w, http.StatusInternalServerError, map[string]string{"error": errorMsg})
}

func responseWithApplicationError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"message": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// GetJob returns the job status
func GetJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID, ok := vars["id"]
	if !ok {
		w.WriteHeader(400)
		return
	}

	k8sScheduler, err := createScheduler()
	if err != nil {
		responseWithError(w, err)
		return
	}

	jobs, err := k8sScheduler.SearchJobs(jobID, CreatedByLabelName, CreatedByLabelValue)
	if err != nil {
		responseWithError(w, err)
		return
	}

	if len(jobs.Items) == 0 {
		w.WriteHeader(404)
		return
	}

	jobStatus := createJob(jobs.Items[0])

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(jobStatus)
	if err != nil {
		responseWithError(w, err)
		return
	}
}

// GetJobList returns the all jobs
func GetJobList(w http.ResponseWriter, r *http.Request) {

	k8sScheduler, err := createScheduler()
	if err != nil {
		responseWithError(w, err)
		return
	}

	allJobs, err := k8sScheduler.SearchJobs("", CreatedByLabelName, CreatedByLabelValue)
	if err != nil {
		responseWithError(w, err)
		return
	}

	result := make([]Job, 0)

	for _, job := range allJobs.Items {
		result = append(result, createJob(job))
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		responseWithError(w, err)
		return
	}
}

// DeleteJob removes a job
func DeleteJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID, ok := vars["id"]
	if !ok {
		w.WriteHeader(400)
		return
	}

	k8sScheduler, err := createScheduler()
	if err != nil {
		responseWithError(w, err)
		return
	}

	err = k8sScheduler.DeleteJob(jobID)
	if err != nil {
		responseWithError(w, err)
		return
	}

	w.WriteHeader(202)
}

func createJob(job batchv1.Job) Job {
	storageBlobPrefix := ""
	if storageBlobPrefixBytes, err := base64.StdEncoding.DecodeString(job.ObjectMeta.Labels[StorageBlobPrefixLabelName]); err == nil {
		if storageBlobPrefixBytes != nil && len(storageBlobPrefixBytes) > 0 {
			storageBlobPrefix = string(storageBlobPrefixBytes)
		}
	}

	result := Job{
		ID:                job.Name,
		Active:            int(job.Status.Active),
		Succeeded:         int(job.Status.Succeeded),
		Failed:            int(job.Status.Failed),
		StorageContainer:  job.ObjectMeta.Labels[StorageContainerLabelName],
		StorageBlobPrefix: storageBlobPrefix,
	}

	result.RunningOnACI = len(job.ObjectMeta.Labels[scheduler.LabelNameVirtualKubelet]) > 0

	if len(job.Status.Conditions) > 0 {
		result.Status = string(job.Status.Conditions[0].Type)
	} else {
		if result.Active > 0 || result.Succeeded > 0 || result.Failed > 0 {
			result.Status = "Pending"
		} else {
			result.Status = "Not Started"
		}

	}

	if job.Status.StartTime != nil {
		t := job.Status.StartTime.UTC()
		result.StartTime = &t
	}

	if job.Status.CompletionTime != nil {
		t := job.Status.CompletionTime.UTC()
		result.CompletionTime = &t
	}

	if job.Spec.Parallelism != nil {
		result.Parallelism = int(*job.Spec.Parallelism)
	}

	if job.Spec.Completions != nil {
		result.Completions = int(*job.Spec.Completions)
	}

	return result
}
