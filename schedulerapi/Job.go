package schedulerapi

import (
	"strconv"
	"time"

	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
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
	Parts             int        `json:"parts,omitempty"`
	Completions       int        `json:"completions,omitempty"`
	StorageContainer  string     `json:"storageContainer,omitempty"`
	StorageBlobPrefix string     `json:"storageBlobPrefix,omitempty"`
	RunningOnACI      bool       `json:"runningOnAci"`
}

// NewJob creates a job from a existing K8s job
func NewJob(job batchv1.Job) Job {
	storageBlobPrefix := decodeBlobNamePrefix(job.ObjectMeta.Labels[StorageBlobPrefixLabelName])

	result := Job{
		ID:                job.Name,
		Active:            int(job.Status.Active),
		Succeeded:         int(job.Status.Succeeded),
		Failed:            int(job.Status.Failed),
		StorageContainer:  job.ObjectMeta.Labels[StorageContainerLabelName],
		StorageBlobPrefix: storageBlobPrefix,
	}

	partsValue := job.ObjectMeta.Labels[PartsLabelName]
	if len(partsValue) > 0 {
		parts, err := strconv.Atoi(partsValue)
		if err == nil {
			result.Parts = parts
		}
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
