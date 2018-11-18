package schedulerapi

import (
	"strconv"
	"time"

	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
	batchv1 "k8s.io/api/batch/v1"
)

// Job defines a working item
type Job struct {
	ID                string                         `json:"id"`
	Status            string                         `json:"status"`
	StartTime         *time.Time                     `json:"startTime,omitempty"`
	CompletionTime    *time.Time                     `json:"completionTime,omitempty"`
	Active            int                            `json:"active,omitempty"`
	Succeeded         int                            `json:"succeeded,omitempty"`
	Failed            int                            `json:"failed,omitempty"`
	Parallelism       int                            `json:"parallelism,omitempty"`
	Completions       int                            `json:"completions,omitempty"`
	StorageContainer  string                         `json:"storageContainer,omitempty"`
	StorageBlobPrefix string                         `json:"storageBlobPrefix,omitempty"`
	ExecutionLocation scheduler.JobExecutionLocation `json:"executionLocation"`
}

// NewJob creates a job from a existing K8s job
func NewJob(job, finalizerJob *batchv1.Job) Job {
	storageBlobPrefix := decodeBlobNamePrefix(job.ObjectMeta.Annotations[StorageBlobPrefixAnnotationName])

	result := Job{
		ID:                job.Name,
		Active:            int(job.Status.Active),
		Succeeded:         int(job.Status.Succeeded),
		Failed:            int(job.Status.Failed),
		StorageContainer:  job.ObjectMeta.Annotations[StorageContainerAnnotationName],
		StorageBlobPrefix: storageBlobPrefix,
	}

	hasFinalizer, _ := strconv.ParseBool(job.ObjectMeta.Annotations[JobHasFinalizerAnnotationName])
	result.ExecutionLocation = scheduler.JobExecutionLocation(job.ObjectMeta.Annotations[scheduler.AnnotationExecutionLocation])

	if job.Status.StartTime != nil {
		t := job.Status.StartTime.UTC()
		result.StartTime = &t
	}

	if job.Spec.Parallelism != nil {
		result.Parallelism = int(*job.Spec.Parallelism)
	}

	if job.Spec.Completions != nil {
		result.Completions = int(*job.Spec.Completions)
	}

	jobToBuildStatus := job

	if hasFinalizer {
		result.Completions++
		jobToBuildStatus = finalizerJob

		if finalizerJob != nil {
			result.Succeeded += int(finalizerJob.Status.Succeeded)
			result.Active += int(finalizerJob.Status.Active)
			result.Failed += int(finalizerJob.Status.Failed)

			if finalizerJob.Status.CompletionTime != nil {
				t := finalizerJob.Status.CompletionTime.UTC()
				result.CompletionTime = &t
			}
		}
	} else {
		if job.Status.CompletionTime != nil {
			t := job.Status.CompletionTime.UTC()
			result.CompletionTime = &t
		}
	}

	// Build status
	if jobToBuildStatus != nil && len(jobToBuildStatus.Status.Conditions) > 0 {
		result.Status = string(jobToBuildStatus.Status.Conditions[0].Type)
	} else {
		if result.Active > 0 || result.Succeeded > 0 || result.Failed > 0 {
			result.Status = "Pending"
		} else {
			result.Status = "Not Started"
		}
	}

	return result
}
