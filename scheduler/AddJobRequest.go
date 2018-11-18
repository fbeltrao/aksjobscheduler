package scheduler

import apiv1 "k8s.io/api/core/v1"

// JobExecutionLocation defines the locations where a job can be executed
type JobExecutionLocation string

// JobExecutionInCluster defines job execution in cluster nodes
const JobExecutionInCluster JobExecutionLocation = "cluster"

// JobExecutionInACI defines job execution in ACI/Virtual Kubelet
const JobExecutionInACI JobExecutionLocation = "aci"

// JobExecutionInAzureBatch defines executing in Azure Batch
const JobExecutionInAzureBatch JobExecutionLocation = "azurebatch"

// AddJobRequest defines properties for new job
type AddJobRequest struct {
	JobID              string
	JobName            string
	Annotations        map[string]string
	Labels             map[string]string
	ImageName          string
	Image              string
	ImagePullSecrets   string
	ImageOS            string
	Parallelism        int
	Completions        int
	ExecutionLocation  JobExecutionLocation
	Commands           []string
	Memory             string
	CPU                string
	Env                []apiv1.EnvVar
	ServiceAccountName string
}

// AddEnv adds an environment variable to the job detail
func (n *AddJobRequest) AddEnv(name, value string) *AddJobRequest {
	newVar := &apiv1.EnvVar{
		Name:  name,
		Value: value,
	}

	n.Env = append(n.Env, *newVar)
	return n
}
