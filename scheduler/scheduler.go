package scheduler

import (
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// LabelNameVirtualKubelet defines the label name for jobs created in ACI
const LabelNameVirtualKubelet = "virtual-kubelet"

// Scheduler facilitates the work with K8s jobs
type Scheduler struct {
	BackoffLimit int32
	Namespace    string
	clientset    *kubernetes.Clientset
}

// NewScheduler creates a new Scheduler
func NewScheduler(kubeconfig string) (*Scheduler, error) {

	var config *rest.Config
	var err error

	if len(kubeconfig) > 0 {
		// use the current context in kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	} else {
		// creates the in-cluster config
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	js := Scheduler{
		BackoffLimit: 4,
		Namespace:    "default",
		clientset:    clientset,
	}
	return &js, nil
}

// NewJob creates a new job
func (s Scheduler) NewJob(jobDetail *NewJobDetail) (*batchv1.Job, error) {

	resourceRequests := apiv1.ResourceList{}

	if len(jobDetail.CPU) > 0 {
		resourceRequests[apiv1.ResourceCPU] = resource.MustParse(jobDetail.CPU)
	}

	if len(jobDetail.Memory) > 0 {
		resourceRequests[apiv1.ResourceMemory] = resource.MustParse(jobDetail.Memory)
	}

	parallelismInt32 := int32(jobDetail.Parallelism)
	completionsInt32 := int32(jobDetail.Completions)

	var imagePullSecrets []apiv1.LocalObjectReference
	if len(jobDetail.ImagePullSecrets) > 0 {
		imagePullSecrets = append(imagePullSecrets, apiv1.LocalObjectReference{
			Name: jobDetail.ImagePullSecrets,
		})
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobDetail.JobID,
			Namespace: s.Namespace,
			Labels:    jobDetail.Labels,
		},
		Spec: batchv1.JobSpec{
			Parallelism:  &parallelismInt32,
			BackoffLimit: &s.BackoffLimit,
			Completions:  &completionsInt32,
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: jobDetail.JobName,
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:    jobDetail.ImageName,
							Image:   jobDetail.Image,
							Command: jobDetail.Commands,
							Env:     jobDetail.Env,
							Resources: apiv1.ResourceRequirements{
								Requests: resourceRequests,
							},
						},
					},
					RestartPolicy:    apiv1.RestartPolicyOnFailure,
					ImagePullSecrets: imagePullSecrets,
				},
			},
		},
	}

	// need to run on ACI?
	if jobDetail.RequiresACI {

		imageOS := "linux"
		if len(jobDetail.ImageOS) > 0 {
			imageOS = strings.ToLower(jobDetail.ImageOS)
		}

		job.Spec.Template.Spec.NodeSelector = map[string]string{
			"beta.kubernetes.io/os": imageOS,
			"kubernetes.io/role":    "agent",
			"type":                  "virtual-kubelet",
		}

		job.Spec.Template.Spec.Tolerations = []apiv1.Toleration{
			{
				Key:      "virtual-kubelet.io/provider",
				Operator: apiv1.TolerationOpEqual,
				Value:    "azure",
				Effect:   apiv1.TaintEffectNoSchedule,
			},
		}

		if job.ObjectMeta.Labels == nil {
			job.ObjectMeta.Labels = make(map[string]string)
		}
		job.ObjectMeta.Labels[LabelNameVirtualKubelet] = "true"
	}

	return s.clientset.Batch().Jobs(s.Namespace).Create(job)
}

// ListJobs retrieves the list of jobs
func (s Scheduler) ListJobs() (*batchv1.JobList, error) {
	return s.clientset.Batch().Jobs(s.Namespace).List(metav1.ListOptions{})
}

// ListJobsByLabel retrieves the list of jobs according to a label
func (s Scheduler) ListJobsByLabel(name, value string) (*batchv1.JobList, error) {
	return s.clientset.Batch().Jobs(s.Namespace).List(metav1.ListOptions{
		LabelSelector: name + "=" + value,
	})
}

// SearchJobs retrieves the list of jobs according to a label and/or name
func (s Scheduler) SearchJobs(jobName, labelName, labelValue string) (*batchv1.JobList, error) {
	listOptions := metav1.ListOptions{}
	if len(jobName) > 0 {
		listOptions.FieldSelector = "metadata.name=" + jobName
	}

	if len(labelName) > 0 {
		listOptions.LabelSelector = labelName + "=" + labelValue
	}

	return s.clientset.Batch().Jobs(s.Namespace).List(listOptions)
}

// FindJobByName retrieves a job by name
func (s Scheduler) FindJobByName(name string) (*batchv1.JobList, error) {

	return s.clientset.Batch().Jobs(s.Namespace).List(metav1.ListOptions{
		FieldSelector: "metadata.name=" + name,
	})
}

// FindJobByLabel retrieves a job by name
func (s Scheduler) FindJobByLabel(name, value string) (*batchv1.JobList, error) {

	return s.clientset.Batch().Jobs(s.Namespace).List(metav1.ListOptions{
		LabelSelector: name + "=" + value,
	})
}

// DeleteJob removes a job
func (s Scheduler) DeleteJob(name string) error {
	return s.clientset.Batch().Jobs(s.Namespace).Delete(name, &metav1.DeleteOptions{})
}

// ClientSet returns the client set
func (s Scheduler) ClientSet() *kubernetes.Clientset {
	return s.clientset
}
