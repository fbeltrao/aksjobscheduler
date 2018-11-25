package schedulerapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Job Scheduler API v1.1")
}

// createJobHandler produces a new job by splitting the uploaded file
func createJobHandler(w http.ResponseWriter, r *http.Request) {

	batchFlag := r.URL.Query().Get("batch")
	useBatch := len(batchFlag) > 0 && batchFlag == "1"
	forceAci := r.URL.Query().Get("aci") == "1"
	gpuType := r.URL.Query().Get("gpu")
	gpuQuantity, err := strconv.Atoi(r.URL.Query().Get("gpuQuantity"))
	if err != nil {
		gpuQuantity = 0
	}

	log.Infof("Received request to create job, content-size: %d, content-type is %s, use batch: %s", r.ContentLength, r.Header.Get("Content-Type"), strconv.FormatBool(useBatch))

	date := time.Now()
	jobUniqueID := randomString()
	jobID := fmt.Sprintf("%s-%s", date.Format("2006-01"), jobUniqueID)
	blobNamePrefix := fmt.Sprintf("%s/%s", date.Format("2006-01"), jobUniqueID)

	locationsCount, err := createJobInputFiles(r, *containerName, blobNamePrefix, NewInputSplitterByLine(*itemsPerJob))
	if err != nil {
		responseWithError(w, err)
		return
	}

	// start job based on input and amount of files created
	log.Printf("Finished creating file, found %d locations", locationsCount)

	if locationsCount < 1 {
		responseWithApplicationError(w, http.StatusBadRequest, "Input file generated 0 jobs")
		return
	}

	if useBatch {
		_, err = createBatchJob(jobID, *containerName, blobNamePrefix, locationsCount)
		if err != nil {
			responseWithError(w, err)
			return
		}
	} else {
		_, err = createKubernetesJob(jobID, *containerName, blobNamePrefix, locationsCount, forceAci, gpuType, gpuQuantity)
		if err != nil {
			responseWithError(w, err)
			return
		}
	}

	w.WriteHeader(202)
	w.Write([]byte(jobID))
}

func getSelectorLabelForJobID(jobID string) string {
	return fmt.Sprintf("%s=%s,%s=%s", scheduler.CreatedByLabelName, scheduler.CreatedByLabelValue, JobCorrelationIDLabelName, jobID)
}

// getJobHandler returns the job status
func getJobHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID, ok := vars["id"]
	if !ok {
		responseWithApplicationError(w, http.StatusBadRequest, "Missing job id")
		return
	}

	k8sScheduler, err := createScheduler()
	if err != nil {
		responseWithError(w, err)
		return
	}

	k8sJobs, err := k8sScheduler.SearchJobsByLabel(getSelectorLabelForJobID(jobID))
	if err != nil {
		responseWithError(w, err)
		return
	}

	if len(k8sJobs.Items) == 0 {
		responseWithApplicationError(w, http.StatusNotFound, "Job not found")
		return
	}

	jobs := createJobsFromList(k8sJobs.Items)

	if len(jobs) == 0 {
		responseWithApplicationError(w, http.StatusNotFound, "Job not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(jobs[0])
	if err != nil {
		responseWithError(w, err)
		return
	}
}

func createJobsFromList(source []batchv1.Job) []Job {

	result := make([]Job, 0)
	for _, item := range source {
		if item.Annotations[MainJobAnnotationName] == "1" {
			j := buildMainJob(item, source)
			result = append(result, j)
		}
	}

	return result
}

func buildMainJob(mainJob batchv1.Job, source []batchv1.Job) Job {

	hasFinalizer, _ := strconv.ParseBool(mainJob.Annotations[JobHasFinalizerAnnotationName])

	var finalizerJob *batchv1.Job
	if hasFinalizer {
		for _, j := range source {
			if j.GetName() == getFinalizerJobID(mainJob.GetName()) {
				finalizerJob = &j
				break
			}
		}
	}

	return NewJob(&mainJob, finalizerJob)
}

// listJobsHandler returns the all jobs
func listJobsHandler(w http.ResponseWriter, r *http.Request) {

	k8sScheduler, err := createScheduler()
	if err != nil {
		responseWithError(w, err)
		return
	}

	allK8sJobs, err := k8sScheduler.SearchJobs("", scheduler.CreatedByLabelName, scheduler.CreatedByLabelValue)
	if err != nil {
		responseWithError(w, err)
		return
	}

	result := createJobsFromList(allK8sJobs.Items)

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(result)
	if err != nil {
		responseWithError(w, err)
		return
	}
}

// deleteJobHandler removes a job
func deleteJobHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID, ok := vars["id"]
	if !ok {
		responseWithApplicationError(w, http.StatusBadRequest, "Missing job id")
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

// getJobResultHandler returns the job result
func getJobResultHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID, ok := vars["id"]
	if !ok {
		responseWithApplicationError(w, http.StatusBadRequest, "Missing job id")
		return
	}

	k8sScheduler, err := createScheduler()
	if err != nil {
		responseWithError(w, err)
		return
	}

	k8sJobs, err := k8sScheduler.SearchJobsByLabel(getSelectorLabelForJobID(jobID))
	if err != nil {
		responseWithError(w, err)
		return
	}

	if len(k8sJobs.Items) == 0 {
		responseWithApplicationError(w, http.StatusNotFound, "Job not found")
		return
	}

	jobs := createJobsFromList(k8sJobs.Items)

	if len(jobs) == 0 {
		responseWithApplicationError(w, http.StatusNotFound, "Job not found")
		return
	}

	job := jobs[0]
	if job.Completions != job.Succeeded {
		responseWithApplicationError(w, http.StatusNotFound, fmt.Sprintf("Job has not yet completed: %d of %d completed", job.Succeeded, job.Completions))
		return
	}

	// open files and stream back
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json")
	err = writeAzureBlobs(r.Context(), w, job.StorageContainer, job.StorageBlobPrefix)
	if err != nil {
		responseWithError(w, err)
		return
	}
}
