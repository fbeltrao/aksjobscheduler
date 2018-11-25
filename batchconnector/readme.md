# Implementation of an Azure Batch connection with AKS

This sample enable sending a Map-Reduce type job to Azure Batch from Kubernetes by using environment variables to set up the desired workers, inputs and outpus. Offloading work to Azure Batch can be interresting for cost optimization as it enables usage of Azure Low priorities Virtual Machines from Kubernetes.
It will start a number of containerized worker jobs depending on the 'completions' environment variable. Once all them are finished successfully, it will start an 'aggregator' job to aggregate the results (based on another container image).

## Batch Setup
To run the sample you will need to:
* Provision a Batch account
* Link the Azure Batch with the container registry you are using (if it is private)
* Preprovision a pool on your Azure Batch with container support 
* Make sure you provision the necessary environment variable to run the sample

## Environment variables needed
* JOBID: the desired Batch Job Id
* POOLID: the desired pool id on which you want the Job to run on. It has to be prepopulated currently (the preparePool Method can help you implement an in code request)
* WORKERIMAGENAME: The image name of the desired jobs
* STORAGEACCOUNT: The storage account where the jobs inputs and outputs are stored
* STORAGEKEY: The key of the storage account where inputs and outputs are stored
* BLOBPREFIX: The desired path of the input/output blob
* CONTAINER: Container name of the blob storage
* AGGREGATORIMAGENAME: Image name of the job running on the aggragator
* COMPLETIONS: Number of parrallel jobs needed to run
* REGISTRYNAME: The URL of the Docker registry containing the aggregator and workers docker images
* REGISTRYUSERNAME: The username of your docker registry 
* REGISTRYPASSWORD: The password of your docker registry
* BATCHCONNECTIONSTRING: The batch connection full qualified connection string
* BATCHNAME: The name of the batch instance 
* BATCHPASSWORD: The password of the batch instance