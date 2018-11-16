package main

// JobFinishedEvent defines a job finished event
type JobFinishedEvent struct {
	ContainerName      string `json:"containerName"`
	JobID              string `json:"jobID"`
	BlobPrefix         string `json:"blobPrefix"`
	StorageAccountName string `json:"storageAccountName"`
}
