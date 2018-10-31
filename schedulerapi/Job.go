package schedulerapi

import "time"

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
