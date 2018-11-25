package scheduler

import (
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// copied from https://github.com/virtual-kubelet/virtual-kubelet/blob/aci-gpu/providers/azure/aci.go
// use it from original source once it is merged to master
const (
	gpuResourceName   apiv1.ResourceName = "nvidia.com/gpu"
	gpuTypeAnnotation                    = "virtual-kubelet.io/gpu-type"
)

// AnnotationExecutionLocation defines the annotation containing the job execution location
const AnnotationExecutionLocation = "execution_location"

// CreatedByLabelName defines the label name for jobs created by this api
const CreatedByLabelName = "created_by"

// CreatedByLabelValue defines the label value for jobs created by this api
const CreatedByLabelValue = "aksscheduler"

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
func (s Scheduler) NewJob(request *AddJobRequest) (*batchv1.Job, error) {

	resourceRequests := apiv1.ResourceList{}
	resourceLimits := apiv1.ResourceList{}

	hasGpu := len(request.GpuType) > 0 && request.GpuQuantity > 0

	if len(request.CPU) > 0 {
		cpuResource := resource.MustParse(request.CPU)
		resourceRequests[apiv1.ResourceCPU] = cpuResource
		if request.ExecutionLocation != JobExecutionInACI {
			resourceLimits[apiv1.ResourceCPU] = cpuResource
		}
	}

	if hasGpu {
		gpuQuantity := resource.MustParse(strconv.Itoa(request.GpuQuantity))
		resourceRequests[gpuResourceName] = gpuQuantity
		resourceLimits[gpuResourceName] = gpuQuantity
	}

	if len(request.Memory) > 0 {
		memoryResource := resource.MustParse(request.Memory)
		resourceRequests[apiv1.ResourceMemory] = memoryResource
		if request.ExecutionLocation != JobExecutionInACI {
			resourceLimits[apiv1.ResourceMemory] = memoryResource
		}
	}

	parallelismInt32 := int32(request.Parallelism)
	completionsInt32 := int32(request.Completions)

	var imagePullSecrets []apiv1.LocalObjectReference
	if len(request.ImagePullSecrets) > 0 {
		imagePullSecrets = append(imagePullSecrets, apiv1.LocalObjectReference{
			Name: request.ImagePullSecrets,
		})
	}

	// Copy annotations adding the execution location
	annotations := map[string]string{
		AnnotationExecutionLocation: string(request.ExecutionLocation),
	}
	for k, v := range request.Annotations {
		annotations[k] = v
	}

	if len(request.GpuType) > 0 {
		annotations[gpuTypeAnnotation] = request.GpuType
	}

	// Copy labels adding the execution location
	labels := map[string]string{
		CreatedByLabelName: CreatedByLabelValue,
	}
	for k, v := range request.Labels {
		labels[k] = v
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        request.JobID,
			Namespace:   s.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			Parallelism:  &parallelismInt32,
			BackoffLimit: &s.BackoffLimit,
			Completions:  &completionsInt32,
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: request.JobName,
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:    request.ImageName,
							Image:   request.Image,
							Command: request.Commands,
							Env:     request.Env,
							Resources: apiv1.ResourceRequirements{
								Requests: resourceRequests,
								Limits:   resourceLimits,
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
	if request.ExecutionLocation == JobExecutionInACI {

		imageOS := "linux"
		if len(request.ImageOS) > 0 {
			imageOS = strings.ToLower(request.ImageOS)
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

// SearchJobsByLabel retrieves the list of jobs according to labels
func (s Scheduler) SearchJobsByLabel(labelSelector string) (*batchv1.JobList, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: labelSelector,
	}

	return s.clientset.Batch().Jobs(s.Namespace).List(listOptions)
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
