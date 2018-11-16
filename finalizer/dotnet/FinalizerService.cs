using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace Finalizer
{
  
    public class FinalizerService : IHostedService
    {        
        private readonly IConfiguration configuration;
        private readonly ILogger logger;
        private readonly string containerName;
        private readonly string blobPrefix;
        private readonly string jobId;
        private CloudStorageAccount cloudStorageAccount;
     
        private Task workerTask;
     
        public FinalizerService(IConfiguration configuration, ILogger<FinalizerService> logger)
        {            
            this.configuration = configuration;
            this.logger = logger;
            this.containerName = configuration.GetValue<string>("STORAGECONTAINER");
            if (string.IsNullOrEmpty(this.containerName))
                throw new Exception("Container name configuration not found");            

            this.blobPrefix = configuration.GetValue<string>("BLOBPREFIX");
            this.jobId = configuration.GetValue<string>("JOBID");

            this.logger.LogInformation("Job Id: {jobId}", this.jobId);
            this.logger.LogInformation("Container name: {containerName}", this.containerName);
            this.logger.LogInformation("Blob prefix: {blobPrefix}", this.blobPrefix);       
            
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                var connectionString = configuration.GetValue<string>("STORAGECONNECTIONSTRING");
                if (string.IsNullOrEmpty(connectionString))
                    throw new Exception("Storage connection string configuration not found");

                this.logger.LogDebug($"Storage connection string: {connectionString}");

                try
                {
                    this.cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
                }
                catch (Exception ex)
                {
                    throw new Exception("Failed to open storage account", ex);
                }

                this.workerTask = Task.Run(() => DoWork(cancellationToken));                           
            }
            catch (Exception ex)
            {
                throw new Exception("Error during job startup", ex); 
            }
            
            return Task.FromResult(0);
        }

        private async Task DoWork(CancellationToken cancellationToken)
        {
            try
            {
                var blobClient = this.cloudStorageAccount.CreateCloudBlobClient();
                var containerReference = blobClient.GetContainerReference(containerName);
                var resultBlobName = Path.Combine(this.blobPrefix, "results.json");
                var resultBlob = containerReference.GetAppendBlobReference(resultBlobName);
                await resultBlob.CreateOrReplaceAsync();

                var currentToken = new BlobContinuationToken();
                
                var outputBlobPrefix = string.Concat(this.blobPrefix, "/output_");

                while (currentToken != null && !cancellationToken.IsCancellationRequested)
                {                
                    var blobSegment = await containerReference.ListBlobsSegmentedAsync(outputBlobPrefix, currentToken);

                    foreach (var item in blobSegment.Results)
                    {                
                        var blobItem = (CloudBlob)item;
                        try
                        {                            
                            await ProcessFile(blobItem, resultBlob);
                            break;
                        }
                        catch (StorageException ex)
                        {                            
                            this.logger.LogCritical("Failed to process blob: {error}", ex.ToString());
                            
                            Program.Shutdown.Cancel();
                            return;
                        }
                    }               

                    currentToken = blobSegment.ContinuationToken;
                }
            }
            catch (Exception ex)
            {
                this.logger.LogCritical("Internal error: {error}", ex.ToString());
                Environment.ExitCode = 1; 
            }
            // Trigger the application shutdown
            Program.Shutdown.Cancel();
        }

        async Task ProcessFile(CloudBlob outputBlob, CloudAppendBlob resultBlob)
        {            
            using (var contentStream = await outputBlob.OpenReadAsync())
            {
                await resultBlob.AppendFromStreamAsync(contentStream);
            }

            this.logger.LogInformation($"Finished processing file '{outputBlob.Name}'");
        }
    
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            this.cancellationTokenSource.Cancel();
            await workerTask;
        }
    }
}
               