# Scheduling Jobs in AKS with Virtual Kubelet

![Scheduling Jobs in AKS with Virtual Kubelet](./media/aksjobscheduler.png)

Most Kubernetes sample applications demonstrate how to run web applications or databases in the orchestrator. Those workloads run until terminated.

However, Kubernetes is also able to run jobs, or in other words, a container that runs for a finite amount of time.

Jobs in Kubernetes can be classified in two:

- [Cron Job](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/): run periodically based on defined schedule
- [Run to Completion](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/): run N amount of times

This repository contains a demo application that schedules `Run to Completion` jobs in a Kubernetes cluster taking advantage of Virtual Kubelet in order to preserved allocated resources.

## Scenario

Company is using a Kubernetes cluster to host a few applications. Cluster resource utilization is high and team wants to avoid oversizing. One of the teams has new requirement to run jobs with unpredicable loads. In high peaks, the required compute resources will exceed what is available in the cluster.

A solution to this problem is to leverage Virtual Kubelet, scheduling jobs outside the cluster in case workload would starve available resources.

## Running containers in Virtual Kubelet in Azure

[Virtual Kubelet](https://github.com/virtual-kubelet/virtual-kubelet) is an open source project allowing Kubernetes to schedule workloads outside of the reserved cluster nodes.

For more information refer to documentation:

- [Azure Container Instance](https://docs.microsoft.com/en-us/azure/container-instances/container-instances-overview)
- [Use Virtual Kubelet in Azure Kubernetes Service](https://docs.microsoft.com/en-us/azure/aks/virtual-kubelet)

## Running jobs in Azure Container Instance

Running a job in Kubernetes is similar to deploying a web application. The difference is how we define the YAML file.

Take for example the [Kubernetes demo job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#running-an-example-job) defined by the YAML below:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pi
spec:
  template:
    spec:
      containers:
      - name: pi
        image: perl
        command: ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
        env:
          - name: "linesPerJob"
            value: 50
      restartPolicy: Never
  backoffLimit: 4
```

Running the job in Kubernetes with kubectl is demostrated below:

```bash
$ kubectl apply -f https://raw.githubusercontent.com/kubernetes/website/master/content/en/examples/controllers/job.yaml
```

Viewing running jobs

```bash
$ kubectl get jobs
NAME           DESIRED   SUCCESSFUL   AGE
pi             1         1            15s
```

Viewing the job result:

```bash
# find first pod where the job name is "pi", then list the logs
POD_NAME=$(kubectl get pods -l "job-name=pi" -o jsonpath='{.items[0].metadata.name}') && clear && kubectl logs $POD_NAME

3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679821480865132823066470938446095505822317253594081284811174502841027019385 REMOVED-TO-SAVE-SPACE
```

To run the same job using Virtual Kubelet add the pod affinity information as the yaml below (if your cluster is not in westeurope you need to change the value of "kubernetes.io/hostname"):

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: aci-pi
spec:
  template:
    spec:
      containers:
      - name: aci-pi
        image: perl
        command: ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
        resources:
          requests:
            cpu: "1"
            memory: "1Gi"
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: virtual-kubelet-virtual-kubelet-linux-westeurope
      tolerations:
      - key: virtual-kubelet.io/provider
        operator: Equal
        value: azure
        effect: NoSchedule
  backoffLimit: 4
```

## Sample application

The code found in this repository has 2 components:

- Jobs API (/server)
- Job Worker (/workers/dotnet)

Starting a new job requires using the Jobs API. The POST request should upload a file like the example below:

```json
{ "id": "1", "value1": 3123, "value2": 321311, "op": "+" }
{ "id": "2", "value1": 3123, "value2": 321311, "op": "-" }
{ "id": "3", "value1": 3123, "value2": 321311, "op": "/" }
{ "id": "4", "value1": 3123, "value2": 321311, "op": "*" }
```

The result of the create job request is the jobID. The ultimate result calculated by workers is the following:

```json
{"id":"1","value1":3123.0,"value2":321311.0,"op":"+","result":324434.0}
{"id":"2","value1":3123.0,"value2":321311.0,"op":"-","result":-318188.0}
{"id":"3","value1":3123.0,"value2":321311.0,"op":"/","result":0.009719555197301057}
{"id":"4","value1":3123.0,"value2":321311.0,"op":"*","result":1003454253.0}
```

Besides copying the input file to Azure Storage the Jobs API will create index files that will identity where each N amount of lines start on the input file. Those small index files will be used by workers to [lease](https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob) a range of lines to be processed, preventing parallel workers from processing the same content.

## Running sample application in AKS

1. Create a new storage account (the sample application will store input and output files there)

2. Deploy application to your AKS with the provided deployment.yaml file. Keep in mind that the deployment will create a new public IP since the service is of type LoadBalancer. Modify the deployment.yaml file to contain your storage credentials.

```bash
kubectl apply -f deployment.yaml
```

2. Watch the Jobs API logs

```bash
POD_NAME=$(kubectl get pods -l "app=jobscheduler" -o jsonpath='{.items[0].metadata.name}') && clear && kubectl logs $POD_NAME -f
```

3. Create a new job by uploading an input file (modify the file path)

```bash
curl -X POST \
  http://{location-of-jobs-api}/jobs \
  -H 'Content-Type: multipart/form-data' \
  -H 'cache-control: no-cache' \
  -H 'content-type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW' \
  -F file=@/path/to/sample_large_work.json
```

4. Look at the job status

```bash
curl http://{location-of-jobs-api}/jobs/{job-id}

{"id":"2018-10-4610526630846599105","status":"Complete","startTime":"2018-10-31T12:31:52Z","completionTime":"2018-10-31T12:33:57Z","succeeded":13,"parallelism":4,"parts":13,"completions":13,"storageContainer":"jobs","storageBlobPrefix":"2018-10/4610526630846599105","runningOnAci":true}
```

5. Download the job result once finished in parts or as a single file (remove the parts query string parameter)

```bash
curl http://{location-of-jobs-api}/jobs/{job-id}/results?part=13
```

### Jobs Web API

Besides interacting with Azure Storage, the API schedules jobs in Kubernetes using the client API ([using the go-client](https://github.com/kubernetes/client-go)).

|Action|Description|
|-|-|
|POST /jobs|Receives a file upload, copy to az storage, create index files and starts K8s job|
|GET /jobs/{id}|Retrieves detail of a job|
|GET /jobs|Retrieves all jobs|

The job creation process is the following:

```text
While (reads line from input file)
  Add line to local buffer (~4 MB)
  Whenever N lines were read, create a index file
  Whenever local buffer is almost full write to blob
End While

Create Kubernetes job based on total_lines. Data returned from the Jobs API is retrieved from the Azure Storage or Kubernetes objects & metadata.
```

#### Jobs API configuration

The API can be configures using environment variables as detailed here:

|Environment Variable|Description|Default value|
|-|-|-|
|STORAGEACCOUNT|Storage account name||
|STORAGEKEY|Storage account key||
|CONTAINERNAME|Storage container name|jobs|
|MAXPARALLELISM|Max parallelism for local cluster|2|
|ACIMAXPARALLELISM|Max parallelism for ACI|50|
|LINESPERJOB|Lines per job|100'000|
|ACICOMPLETIONSTRIGGER|Defines the amount of completions necessary to execute the job using virtual kubelet|Default is 6. 0 to disable ACI|
|ACIHOSTNAME|Defines the ACI host name|virtual-kubelet-virtual-kubelet-linux-westeurope|
|JOBIMAGE|Image to be used in job|fbeltrao/aksjobscheduler-worker-dotnet:1.0|
|JOBCPULIMIT|Job CPU limit for local cluster|0.5|
|ACIJOBCPULIMIT|Job CPU limit for ACI|1|
|JOBMEMORYLIMIT|Job Memory limit for local cluster|256Mi|
|ACIJOBMEMORYLIMIT|Job Memory limit for ACI|1Gi|
|EVENTGRIDENDPOINT|Event Grid event endpoint to publish when job is done||
|EVENTGRIDSASKEY|Event Grid sas key publish when job is done||
|DEBUGLOG|Enabled debug log level|false|

Source code is located at /server

### Worker

The provided worker implementation is based on .NET Core. A worker execution happens in the following way:

```text
If could acquire lease to one of the index files
  Periodically renew lease
  Process lines defined by leased index file, writing to output file
  Delete leased index file
End If
```

Source code is located at /workers/dotnet/

### Event grid integration

There are two ways to identify when a job has finished:

- By polling the API /jobs/{id}

```json
{
    "id": "2018-10-8405256245329624086",
    "status": "Complete",
    "startTime": "2018-10-30T15:43:10Z",
    "completionTime": "2018-10-30T15:43:22Z",
    "succeeded": 2,
    "parallelism": 1,
    "completions": 2,
    "storageContainer": "jobs",
    "storageBlobPrefix": "2018-10/8405256245329624086",
    "runningOnAci": false
}
```

- By using Event Grid. If an event grid endpoint and sas keys was provided to the Jobs API an additional completion will be scheduled. The worker will send an event grid notification if no index files is found (all indexes were completed).
