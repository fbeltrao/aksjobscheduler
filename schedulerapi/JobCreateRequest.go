package schedulerapi

// JobCreateRequest defines a request to create a job where the files already exist
type JobCreateRequest struct {
	ID                string `json:"id"`
	StorageContainer  string `json:"storageContainer,omitempty"`
	StorageBlobPrefix string `json:"storageBlobPrefix,omitempty"`
}
