package schedulerapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Job Scheduler API")
}

// createJobHandler produces a new job by splitting the uploaded file
func createJobHandler(w http.ResponseWriter, r *http.Request) {

	log.Printf("Received request to create job, content-size: %d, content-type is %s\n", r.ContentLength, r.Header.Get("Content-Type"))

	date := time.Now()
	jobUniqueID := randomString()
	jobID := fmt.Sprintf("%s-%s", date.Format("2006-01"), jobUniqueID)
	blobNamePrefix := fmt.Sprintf("%s/%s", date.Format("2006-01"), jobUniqueID)

	linesCount, err := createJobInputFile(r, *containerName, blobNamePrefix)
	if err != nil {
		responseWithError(w, err)
		return
	}

	// start job based on input and amount of files created
	log.Printf("Finished creating file, found %d lines", linesCount)

	err = createJobFromInputLines(jobID, *containerName, blobNamePrefix, linesCount)
	if err != nil {
		responseWithError(w, err)
		return
	}

	w.WriteHeader(202)
	w.Write([]byte(jobID))
}

// getJobHandler returns the job status
func getJobHandler(w http.ResponseWriter, r *http.Request) {
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

// listJobsHandler returns the all jobs
func listJobsHandler(w http.ResponseWriter, r *http.Request) {

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

// deleteJobHandler removes a job
func deleteJobHandler(w http.ResponseWriter, r *http.Request) {
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
