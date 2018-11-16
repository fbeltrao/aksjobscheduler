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
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace Worker
{
    public enum FindAndLeaseInputBlobResultType
    {
        LeaseFailed,
        NoFilesFound,
        LeaseAcquired,

    }

    public class JobService : IHostedService
    {
        const int LEASE_TIME_IN_SECONDS = 30;
        const int RENEW_LEASE_INTERVAL_IN_SECONDS = 15;
        
        private readonly IConfiguration configuration;
        private readonly ILogger logger;
        private readonly string containerName;
        private readonly string blobPrefix;
        private readonly int itemsPerJob;
        private readonly string jobId;
        private readonly string storageAccountName;
        private readonly string storageAccountKey;
        private CloudStorageAccount cloudStorageAccount;
        private CloudBlob ctrlCloudBlob;
        private string leaseId;

        private Task workerTask;
        private Task renewTask;

        public JobService(IConfiguration configuration, ILogger<JobService> logger)
        {            
            this.configuration = configuration;
            this.logger = logger;
            this.containerName = configuration.GetValue<string>("CONTAINER");
            if (string.IsNullOrEmpty(this.containerName))
                throw new Exception("Container name configuration not found");            

            this.blobPrefix = configuration.GetValue<string>("BLOBPREFIX");
            this.itemsPerJob = configuration.GetValue<int>("ITEMSPERJOB");
            this.jobId = configuration.GetValue<string>("JOBID");
            this.storageAccountName = configuration.GetValue<string>("STORAGEACCOUNT");
            this.storageAccountKey = configuration.GetValue<string>("STORAGEKEY");            

            this.logger.LogInformation("Job Id: {jobId}", this.jobId);
            this.logger.LogInformation("Container name: {containerName}", this.containerName);
            this.logger.LogInformation("Blob prefix: {blobPrefix}", this.blobPrefix);
            this.logger.LogInformation("Lines per job: {itemsPerJob}", this.itemsPerJob);
            this.logger.LogInformation("Storage account: {storageAccountName}", this.storageAccountName);
            this.logger.LogDebug("Storage account key: {storageAccountKey}", this.storageAccountKey);
            
        }

        async Task<FindAndLeaseInputBlobResultType> FindAndLeaseInputBlob(CloudBlobContainer containerReference)
        {
            BlobContinuationToken currentToken = new BlobContinuationToken();
            
            var controlBlobPrefix = string.Concat(this.blobPrefix, "/ctrl_input_");
            var firstLoop = true;

            while (currentToken != null)
            {                
                var blobSegment = await containerReference.ListBlobsSegmentedAsync(controlBlobPrefix, currentToken);
                if (firstLoop)
                {
                    if (!blobSegment.Results.Any())
                    {
                        return FindAndLeaseInputBlobResultType.NoFilesFound;
                    }

                    firstLoop = false;
                }

                foreach (var item in blobSegment.Results)
                {
                    // try to adquire lease
                    try
                    {                     
                        this.ctrlCloudBlob = (CloudBlob)item;
                        this.leaseId = await this.ctrlCloudBlob.AcquireLeaseAsync(TimeSpan.FromSeconds(LEASE_TIME_IN_SECONDS));                        
                        this.logger.LogInformation($"Adquired lease '{this.ctrlCloudBlob.Name}' with id {this.leaseId}");
                        return FindAndLeaseInputBlobResultType.LeaseAcquired;
                    }
                    catch (StorageException ex)
                    {
                        // failed to get lease
                        this.logger.LogWarning("Failed to lease blob: {error}", ex.ToString());
                    }
                }               

                currentToken = blobSegment.ContinuationToken;
            }

            return FindAndLeaseInputBlobResultType.LeaseFailed;
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
            const int MAX_LEASE_ATTEMPTS = 3;

            try
            {
                // 1. Lease one of the the files in container
                var blobClient = this.cloudStorageAccount.CreateCloudBlobClient();
                var containerReference = blobClient.GetContainerReference(this.containerName);

                var findLeaseResult = FindAndLeaseInputBlobResultType.LeaseFailed;
                var leaseTry = 1;
                while (leaseTry <= MAX_LEASE_ATTEMPTS)
                {
                    findLeaseResult = await FindAndLeaseInputBlob(containerReference);
                    if (findLeaseResult == FindAndLeaseInputBlobResultType.LeaseFailed)
                    {                    
                        this.logger.LogInformation("Could not find a free control blob in try #{try}", leaseTry);                
                        leaseTry++;

                        // Wait 10, 15 seconds
                        await Task.Delay(TimeSpan.FromSeconds(leaseTry * 5));
                    }
                    else
                    {
                        break;
                    }                
                }

                if (findLeaseResult == FindAndLeaseInputBlobResultType.LeaseFailed)
                {
                    this.logger.LogCritical("Could not lease any blob, stopping");
                    Environment.ExitCode = 1; 
                    Program.Shutdown.Cancel();
                    return;
                }

                if (findLeaseResult == FindAndLeaseInputBlobResultType.NoFilesFound)
                {
                    this.logger.LogInformation("No blob was found!");
                }
                else
                {
                    // 2. Start lease renew thread
                    var stopRenewCancellationToken = new CancellationTokenSource();
                    var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stopRenewCancellationToken.Token);
                    this.renewTask = Task.Run(() => this.RenewLease(cancellationTokenSource.Token));

                    // 3. Process file
                    await this.ProcessFile(cancellationToken);

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        // 4. Delete input file 

                        // first: stops the renew task
                        stopRenewCancellationToken.Cancel();
                        await this.renewTask;
                        
                        await DeleteInputFile();

                        this.logger.LogInformation("Finished succesfully");   
                    }
                }
                    
            }
            catch (Exception ex)
            {
                this.logger.LogCritical(ex, "Job execution failed");
                Environment.ExitCode = 1;  
            }

            // Trigger the application shutdown
            Program.Shutdown.Cancel();
        }

        async Task DeleteInputFile()
        {
            // adquire lease once more
            await this.ctrlCloudBlob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(this.leaseId));                                    
                
            // Now delete it
            var blobName = this.ctrlCloudBlob.Name;
            await this.ctrlCloudBlob.DeleteAsync(
                DeleteSnapshotsOption.None, 
                AccessCondition.GenerateLeaseCondition(this.leaseId),
                new BlobRequestOptions(),
                null);   

            this.logger.LogInformation("Control blob '{blob}' deleted", blobName);
        }


        // Keeps the lease on the blob file as long as the cancellation token is not cancelled
        async Task RenewLease(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await this.ctrlCloudBlob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(this.leaseId));

                    await Task.Delay(TimeSpan.FromSeconds(RENEW_LEASE_INTERVAL_IN_SECONDS), cancellationToken);

                    this.logger.LogInformation("Lease {leaseId} in {blob} renewed", this.leaseId, this.ctrlCloudBlob.Name);
                }
            }
            catch (TaskCanceledException)
            {                
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error renewing lease {leaseId} in {blob}", this.leaseId, this.ctrlCloudBlob.Name);
            }

            this.logger.LogInformation("Exiting renew lease");
        }

        async Task ProcessFile(CancellationToken cancellationToken)
        {
            // Create output blob
            const int MIN_WRITE_BLOCK_SIZE = 1024 * 1024 * 50; // 50 MB
            const int START_OUTPUT_BUFFER_SIZE = MIN_WRITE_BLOCK_SIZE + 1024; // (must be > MIN_WRITE_BLOCK_SIZE)
            var controlFileName = Path.GetFileName(this.ctrlCloudBlob.Name);
            var inputFilePath = Path.GetDirectoryName(this.ctrlCloudBlob.Name);

            // parse the input file name: 'ctrl_input_#counter_#firstByteIndex
            var inputFileValues = controlFileName.Split('_');
            if (inputFileValues.Length != 4)
                throw new Exception($"Control file name is invalid '{controlFileName}'");
            
            if (!int.TryParse(inputFileValues[2], out var fileNumber))
                throw new Exception($"Control file name is invalid '{controlFileName}', could not parse file number");

            if (!int.TryParse(inputFileValues[3], out var firstByteIndex))
                throw new Exception($"Control file name is invalid '{controlFileName}', could not parse first byte index");                

            var outputBlobName = string.Concat(inputFilePath, "/output_", fileNumber, ".json");
            this.logger.LogInformation("Job outpub blob name is {blobOutputName}", outputBlobName);                       

            var outputBlobReference = this.ctrlCloudBlob.Container.GetAppendBlobReference(outputBlobName);
            await outputBlobReference.CreateOrReplaceAsync();
            outputBlobReference.Properties.ContentType = "application/json";
            await outputBlobReference.SetPropertiesAsync();

            var outputBuffer = new StringBuilder(START_OUTPUT_BUFFER_SIZE);

            var inputBlob = this.ctrlCloudBlob.Container.GetAppendBlobReference(string.Concat(this.blobPrefix, "/input.json"));
            
            var linesProcessed = 0;
            var linesRead = 0;
            using (var blobStream = await inputBlob.OpenReadAsync())
            {
                blobStream.Seek(firstByteIndex, SeekOrigin.Begin);
                using (var reader = new StreamReader(blobStream))
                {                                        
                    while ( !reader.EndOfStream && 
                            !cancellationToken.IsCancellationRequested && 
                            linesRead < this.itemsPerJob)
                    {
                        var line = await reader.ReadLineAsync();
                        ++linesRead;
                        try
                        {
                            if (!string.IsNullOrEmpty(line))
                            {
                                var request = JsonConvert.DeserializeObject<CalculationRequest>(line);
                                linesProcessed++;

                                var response = request.Calculate();
                                outputBuffer.AppendLine(JsonConvert.SerializeObject(response));

                                if (outputBuffer.Length > MIN_WRITE_BLOCK_SIZE)
                                {
                                    this.logger.LogInformation($"Writing to output {outputBlobReference.Name} {outputBuffer.Length} chars");
                                    await outputBlobReference.AppendTextAsync(outputBuffer.ToString());
                                    outputBuffer.Length = 0;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            this.logger.LogError(ex, $"Failed to process line: {line}");
                        }
                        
                        
                        if (linesProcessed % 50000 == 0)
                        {
                            this.logger.LogInformation($"Processed {linesProcessed} lines");
                        }                        
                    } 

                    if (outputBuffer.Length > 0)
                    {
                        this.logger.LogInformation($"Writing to output {outputBlobReference.Name} {outputBuffer.Length} chars");
                        await outputBlobReference.AppendTextAsync(outputBuffer.ToString());
                    }                   
                }
            }

            this.logger.LogInformation($"Finished processing file, {linesProcessed} lines");            
        }
    
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            var tasksToWait = this.renewTask == null ? new Task[] { this.workerTask } : new Task[] { this.renewTask, this.workerTask };
            await Task.WhenAll(tasksToWait);
        }
    }
}
               