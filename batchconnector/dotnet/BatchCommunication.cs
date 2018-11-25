using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BatchConnector
{
    public static class BatchCommunication
    {
        private static BatchClient PrepareConnection()
        {
            // Set up the Batch Service credentials used to authenticate with the Batch Service.
            BatchSharedKeyCredentials credentials = new BatchSharedKeyCredentials(
                Environment.GetEnvironmentVariable("BATCHCONNECTIONSTRING"),
                Environment.GetEnvironmentVariable("BATCHNAME"),
                Environment.GetEnvironmentVariable("BATCHPASSWORD"));

            // Get an instance of the BatchClient for a given Azure Batch account.
            BatchClient batchClient = BatchClient.Open(credentials);
            batchClient.CustomBehaviors.Add(RetryPolicyProvider.ExponentialRetryProvider(TimeSpan.FromSeconds(5), 3));
            return batchClient;

        }

        /// <summary>
        /// This method can create a pool, it is not used in our example.
        /// The code remains for documentation purposes.
        /// </summary>
        public static void PreparePool()
        {
            BatchClient batchClient = PrepareConnection();
            if (Environment.GetEnvironmentVariable("REGISTRYNAME") != null)
            {
                ContainerRegistry containerRegistry = new ContainerRegistry(
                    registryServer: Environment.GetEnvironmentVariable("REGISTRYNAME"),
                    userName: Environment.GetEnvironmentVariable("REGISTRYUSERNAME"),
                    password: Environment.GetEnvironmentVariable("REGISTRYPASSWORD")
                    );

                // Create container configuration, prefetching Docker images from the container registry
                ContainerConfiguration containerConfig = new ContainerConfiguration()
                {
                    ContainerImageNames = new List<string>() 
                    {
                        Environment.GetEnvironmentVariable("WORKERIMAGENAME")
                    },
                    ContainerRegistries = new List<ContainerRegistry> 
                    { 
                        containerRegistry 
                    }
                };

                ImageReference imageReference = new ImageReference(
                    publisher: "microsoft-azure-batch",
                    offer: "ubuntu-server-container",
                    sku: "16-04-lts",
                    version: "latest");
                
                // VM configuration
                VirtualMachineConfiguration virtualMachineConfiguration = new VirtualMachineConfiguration(
                    imageReference: imageReference,
                    nodeAgentSkuId: "batch.node.ubuntu 16.04")
                    {
                        ContainerConfiguration = containerConfig,
                    };

                //Create pool
                CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: "docker",
                    targetDedicatedComputeNodes: 1,
                    virtualMachineSize: "Standard_A2_v2",
                    virtualMachineConfiguration: virtualMachineConfiguration);

                pool.Commit();
            }
        }

        /// <summary>
        /// Create a task to submit to the Batch instance
        /// </summary>
        /// <param name="jobId">Desired job id</param>
        /// <param name="imageName">Desired docker image name</param>
        /// <param name="batchClient">The batchclient communicating with the batch instance</param>
        /// <param name="taskEnvironmentSettings">Task's settings</param>
        /// <param name="taskName">The name of the task</param>
        /// <param name="taskDependencies">Dependencies the task has to wait upon</param>
        private static void CreateTask(string jobId, string imageName, BatchClient batchClient, List<EnvironmentSetting> taskEnvironmentSettings,
          string taskName, List<string> taskDependencies)
        {
            // Assign the job preparation task to the job
            TaskContainerSettings workerContainer = new TaskContainerSettings(
            imageName
          );
            CloudTask workerTask = new CloudTask(taskName, "echo here")
            {
                ContainerSettings = workerContainer,
                Id = jobId,
                EnvironmentSettings = taskEnvironmentSettings,
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Task)),
                DependsOn = TaskDependencies.OnIds(taskDependencies)
            };
            // Commit Job to create it in the service
            batchClient.JobOperations.AddTask(jobId, workerTask);
        }

        /// <summary>
        /// Creates a job and adds a task to it.
        /// </summary>
        /// <param name="batchClient">The BatchClient to use when interacting with the Batch service.</param>
        /// <param name="configurationSettings">The configuration settings</param>
        /// <param name="jobId">The ID of the job.</param>
        /// <returns>An asynchronous <see cref="Task"/> representing the operation.</returns>
        public static async Task SubmitJobAsync(String jobId, String poolId, String imageName, int completions, String destinationStorageAccount,
            String destinationStorageAccountKey, String destinationContainerName, String destinationPrefixName, String aggregatorImageName)
        {
            BatchClient batchClient = PrepareConnection();
            System.Console.WriteLine("Connected to Batch service...");
            CloudJob newJob = batchClient.JobOperations.CreateJob();
            newJob.Id = jobId;
            newJob.PoolInformation = new PoolInformation { PoolId = poolId };
            newJob.UsesTaskDependencies = true;
            newJob.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
            await newJob.CommitAsync();

            List<EnvironmentSetting> taskEnvironmentSettings = new List<EnvironmentSetting>() {
                new EnvironmentSetting("STORAGEACCOUNT",destinationStorageAccount),
                 new EnvironmentSetting("STORAGEKEY",destinationStorageAccountKey),
                 new EnvironmentSetting("CONTAINER",destinationContainerName),
                 new EnvironmentSetting("BLOBPREFIX",destinationPrefixName),
                };

            List<String> jobIds = new List<String>();
            for (int i = 0; i < completions; i++)
            {
                CreateTask(jobId, imageName, batchClient, taskEnvironmentSettings, i.ToString(), null);
                jobIds.Add(i.ToString());
                System.Console.WriteLine("Created Task " + i.ToString());
            }
            System.Console.WriteLine("Creating aggregator...");
            CreateTask(jobId, imageName, batchClient, taskEnvironmentSettings, "aggregator", jobIds);
            while (batchClient.JobOperations.GetJob(jobId).State != JobState.Completed)
            {
                Console.WriteLine(System.DateTime.Now + ": In waiting state for job: " + jobId + ", current state: " + batchClient.JobOperations.GetJob(jobId).State);
                Thread.Sleep(5000);
            }
            Console.WriteLine("Job finished");
        }
    }
}
