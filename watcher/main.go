package main

import (
	"flag"
	"path/filepath"
	"time"

	"github.com/Microsoft/ApplicationInsights-Go/appinsights"
	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
)

var (
	appInsightsClient appinsights.TelemetryClient

	// application insights instrumentation key
	instrumentationKey = flag.String("instrumentationKey", getEnvString("INSTRUMENTATION_KEY", ""), "Application Insights instrumentation key")

	// kubeConfig defines the kube config location, only if running outside the cluster
	kubeConfig *string

	// jobName defines the job name
	jobName = flag.String("jobName", getEnvString("JOBNAME", "waiterjob"), "Job name")

	// jobName defines the job name account key
	namespace = flag.String("namespace", getEnvString("NAMESPACE", "default"), "Job namespace")

	// jobImageOS defines the job image OS
	jobImageOS = flag.String("jobImageOS", getEnvString("JOBIMAGEOS", "linux"), "Defines the job image OS (windows, linux). linux by default")

	// jobImage defines the image identifier about the job
	jobImage = flag.String("jobImage", getEnvString("JOBIMAGE", "fbeltrao/aksjobscheduler-worker-dotnet:1.0"), "Image to be used in job. Default (fbeltrao/aksjobscheduler-worker-dotnet:1.0)")

	// JobImagePullSecret defines the image pull secret when using a private image repository
	jobImagePullSecret = flag.String("jobImagePullSecret", getEnvString("JOBIMAGEPULLSECRET", ""), "Defines the image pull secret when using a private image repository")

	// jobCPULimit defines the CPU limit for the job pod when running on local cluster
	jobCPULimit = flag.String("jobCPULimit", getEnvString("JOBCPULIMIT", "0.5"), "Job CPU limit for local cluster (0.5 by default)")

	// JobMemoryLimit defines the memory limit for the job pod when running on local cluster
	jobMemoryLimit = flag.String("jobMemoryLimit", getEnvString("JOBMEMORYLIMIT", "256Mi"), "Job Memory limit for local cluster (256Mi by default)")

	runInACI = flag.Bool("runInACI", getEnvBool("RUNINACI", false), "Defines if the aggregator job should execute in ACI")

	// storageAccountName defines the storage account name
	storageAccountName = flag.String("storageAccount", getEnvString("STORAGEACCOUNT", ""), "Storage account name")

	// storageAccountKey defines the storage account key
	storageAccountKey = flag.String("storageKey", getEnvString("STORAGEKEY", ""), "Storage account key")

	// ContainerName defines the Storage container name
	containerName = flag.String("containerName", getEnvString("CONTAINERNAME", "jobs"), "Storage container name ('jobs' by default)")

	blobPrefix = flag.String("blobPrefix", getEnvString("BLOBPREFIX", ""), "Blob prefix")

	// jobFinishedEventGridTopicEndpoint defines event endpoint to publish when job is done
	jobFinishedEventGridTopicEndpoint = flag.String("eventGridTopicEndpoint", getEnvString("EVENTGRIDENDPOINT", ""), "Event Grid event endpoint to publish when job is done")

	// jobFinishedEventGridSasKey defines sas key to publish when job is done
	jobFinishedEventGridSasKey = flag.String("eventGridSasKey", getEnvString("EVENTGRIDSASKEY", ""), "Event Grid sas key publish when job is done")
)

func createScheduler() (*scheduler.Scheduler, error) {
	return scheduler.NewScheduler("")
	//return scheduler.NewScheduler(*kubeConfig)
}

func main() {
	log.Infof("Starting job watcher: %s", time.Now().Format("2006-01-02 15:04:05"))
	home := homeDir()
	log.Infof("Home dir: %s", home)

	if len(home) > 0 {
		kubeConfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeConfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	log.Infof("Application Insights: %s", *instrumentationKey)
	log.Infof("Job Name: %s", *jobName)
	log.Infof("Kubeconfig: %s", *kubeConfig)
	log.Infof("Storage account: %s", *storageAccountName)
	log.Infof("Storage key: %s", *storageAccountKey)
	log.Infof("Kube config file: %s", *kubeConfig)
	log.Infof("Container name: %s", *containerName)
	log.Infof("Job Finished Event Grid topic endpoint: %s", *jobFinishedEventGridTopicEndpoint)
	log.Infof("Job Finished Event Grid sas key: %s", *jobFinishedEventGridSasKey)

	k8sScheduler, err := createScheduler()
	if err != nil {
		log.Fatal(err)
	}

	err = waitForJobCompletion(k8sScheduler, *jobName)
	if err != nil {
		log.Fatal(err)
	}

	finalizerJob, err := startFinalizerJob(k8sScheduler, *jobName)
	if err != nil {
		log.Fatal(err)
	}

	if jobFinishedEventGridTopicEndpoint != nil &&
		jobFinishedEventGridSasKey != nil &&
		len(*jobFinishedEventGridSasKey) > 0 &&
		len(*jobFinishedEventGridTopicEndpoint) > 0 {

		waitForJobCompletion(k8sScheduler, finalizerJob.GetName())

		ev := EventGridEvent{
			Topic:     "job",
			EventType: "jobfinished",
			ID:        *jobName,
			Subject:   "finished",
			Data: JobFinishedEvent{
				BlobPrefix:         *blobPrefix,
				ContainerName:      *containerName,
				JobID:              *jobName,
				StorageAccountName: *storageAccountName,
			},
		}
		err = ev.Send(*jobFinishedEventGridTopicEndpoint, *jobFinishedEventGridSasKey)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Infof("Ending job watcher: %s", time.Now().Format("2006-01-02 15:04:05"))

}

func waitForJobCompletion(k8sScheduler *scheduler.Scheduler, jobName string) error {
	for {
		log.Infof("Searching for job %s", jobName)
		jobs, err := k8sScheduler.FindJobByName(jobName)
		if err != nil {
			return err
		}

		log.Infof("Search for %s found %d jobs", jobName, len(jobs.Items))

		if len(jobs.Items) > 0 {

			job := jobs.Items[0]

			if len(job.Status.Conditions) == 0 {
				log.Infof("Job has no status conditions")
			} else {
				log.Infof("Job status: %v", job.Status.Conditions[0].Type)

				if job.Status.Conditions[0].Type == batchv1.JobComplete {
					return nil
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func startFinalizerJob(k8sScheduler *scheduler.Scheduler, jobName string) (*batchv1.Job, error) {
	finalizerJobName := jobName + "-finalizer"
	log.Infof("Starting finalizer job '%s', image: %s, cpu: %s, memory: %s", finalizerJobName, *jobImage, *jobCPULimit, *jobMemoryLimit)

	jobDetail := &scheduler.NewJobDetail{
		Completions:      1,
		Parallelism:      1,
		CPU:              *jobCPULimit,
		Memory:           *jobMemoryLimit,
		JobID:            finalizerJobName,
		JobName:          finalizerJobName,
		ImageName:        "jobfinalizer",
		ImagePullSecrets: *jobImagePullSecret,
		ImageOS:          *jobImageOS,
		Image:            *jobImage,
		Labels: map[string]string{
			"job_correlation_id": jobName,
		},
		RequiresACI: *runInACI,
		Env: []v1.EnvVar{
			{Name: "STORAGEACCOUNT", Value: *storageAccountName},
			{Name: "STORAGEKEY", Value: *storageAccountKey},
			{Name: "CONTAINER", Value: *containerName},
			{Name: "BLOBPREFIX", Value: *blobPrefix},
		},
	}

	k8sJob, err := k8sScheduler.NewJob(jobDetail)

	return k8sJob, err
}
