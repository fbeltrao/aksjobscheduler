package scheduler

import apiv1 "k8s.io/api/core/v1"

// NewJobDetail defines properties for new job
type NewJobDetail struct {
	JobID              string
	JobName            string
	Labels             map[string]string
	ImageName          string
	Image              string
	ImagePullSecrets   string
	ImageOS            string
	Parallelism        int
	Completions        int
	RequiresACI        bool
	Commands           []string
	Memory             string
	CPU                string
	Env                []apiv1.EnvVar
	ServiceAccountName string
}

// AddEnv adds an environment variable to the job detail
func (n *NewJobDetail) AddEnv(name, value string) *NewJobDetail {
	newVar := &apiv1.EnvVar{
		Name:  name,
		Value: value,
	}

	n.Env = append(n.Env, *newVar)
	return n
}
