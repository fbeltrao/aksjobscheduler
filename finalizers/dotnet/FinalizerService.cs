using System;
using System.Diagnostics;
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
        private readonly string storageAccountName;
        private readonly string storageAccountKey;
        private CloudStorageAccount cloudStorageAccount;
     
          private Task workerTask;
     
        public FinalizerService(IConfiguration configuration, ILogger<FinalizerService> logger)
        {            
            this.configuration = configuration;
            this.logger = logger;
            this.containerName = configuration.GetValue<string>("CONTAINER");
            if (string.IsNullOrEmpty(this.containerName))
                throw new Exception("Container name configuration not found");            

            this.blobPrefix = configuration.GetValue<string>("BLOBPREFIX");
            this.jobId = configuration.GetValue<string>("JOBID");
            this.storageAccountName = configuration.GetValue<string>("STORAGEACCOUNT");
            this.storageAccountKey = configuration.GetValue<string>("STORAGEKEY");            


            this.logger.LogInformation("Job Id: {jobId}", this.jobId);
            this.logger.LogInformation("Container name: {containerName}", this.containerName);
            this.logger.LogInformation("Blob prefix: {blobPrefix}", this.blobPrefix);       
            this.logger.LogInformation("Storage account: {storageAccountName}", this.storageAccountName);
            this.logger.LogDebug("Storage account key: {storageAccountKey}", this.storageAccountKey);
       
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                if (string.IsNullOrEmpty(this.storageAccountName))
                    throw new Exception("Storage account name configuration not found");

                if (string.IsNullOrEmpty(this.storageAccountKey))
                    throw new Exception("Storage account key configuration not found");

                try
                {
                    this.cloudStorageAccount = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={this.storageAccountName};AccountKey={this.storageAccountKey};EndpointSuffix=core.windows.net");
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

                this.logger.LogInformation($"Starting to create result file: {resultBlobName}");  

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
                        }
                        catch (StorageException ex)
                        {                            
                            this.logger.LogCritical("Failed to process blob: {error}", ex.ToString());
                            Environment.ExitCode = 1;
                            Program.Shutdown.Cancel();
                            return;
                        }
                    }               

                    currentToken = blobSegment.ContinuationToken;
                }

                this.logger.LogInformation("Finished succesfully");  
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
            var sw = Stopwatch.StartNew();
            using (var contentStream = await outputBlob.OpenReadAsync())
            {
                await resultBlob.AppendFromStreamAsync(contentStream);
            }
            sw.Stop();

            this.logger.LogInformation($"Finished processing file '{outputBlob.Name}' into '{resultBlob.Name}' in {sw.ElapsedMilliseconds}ms");
        }
    
        public async Task StopAsync(CancellationToken cancellationToken)
        {            
            await workerTask;
        }
    }
}
               