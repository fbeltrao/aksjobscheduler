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
        private readonly IJobFinishedNotifier jobFinishedNotifier;
        private readonly ILogger logger;
        private readonly string containerName;
        private readonly string blobPrefix;
        private readonly int linesPerJob;
        private readonly string jobId;
        private CloudStorageAccount cloudStorageAccount;
        private CloudBlob ctrlCloudBlob;
        private string leaseId;

        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private Task workerTask;
        private Task renewTask;

        public JobService(IConfiguration configuration, IJobFinishedNotifier jobFinishedNotifier, ILogger<JobService> logger)
        {            
            this.configuration = configuration;
            this.jobFinishedNotifier = jobFinishedNotifier;
            this.logger = logger;
            this.containerName = configuration.GetValue<string>("STORAGECONTAINER");
            if (string.IsNullOrEmpty(this.containerName))
                throw new Exception("Container name configuration not found");            

            this.blobPrefix = configuration.GetValue<string>("BLOBPREFIX");
            this.linesPerJob = configuration.GetValue<int>("LINESPERJOB");
            this.jobId = configuration.GetValue<string>("JOBID");

            this.logger.LogInformation("Job Id: {jobId}", this.jobId);
            this.logger.LogInformation("Container name: {containerName}", this.containerName);
            this.logger.LogInformation("Blob prefix: {blobPrefix}", this.blobPrefix);
            this.logger.LogInformation("Lines per job: {linesPerJob}", this.linesPerJob);
            
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
                    throw new Exception("Failed to create storage account", ex);
                }                

                this.workerTask = Task.Run(() => DoWork());                           
            }
            catch (Exception ex)
            {
                throw new Exception("Error during job startup", ex); 
            }
            
            return Task.FromResult(0);
        }

        private async Task DoWork()
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
                    Environment.ExitCode = 1; 
                    Program.Shutdown.Cancel();
                    return;
                }

                if (findLeaseResult == FindAndLeaseInputBlobResultType.NoFilesFound)
                {
                    await NotifyJobFinished();                    
                }
                else
                {
                    // 2. Start lease renew thread
                    this.renewTask = Task.Run(() => this.RenewLease());

                    // 3. Process file
                    await this.ProcessFile();

                    if (!this.cancellationTokenSource.IsCancellationRequested)
                    {
                        // 4. Delete input file
                        // adquire lease once more
                        await this.ctrlCloudBlob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(this.leaseId));
                            
                        // stop the renew process
                        this.cancellationTokenSource.Cancel();
                        await this.renewTask;
                            
                        // Now delete it
                        var blobName = this.ctrlCloudBlob.Name;
                        await this.ctrlCloudBlob.DeleteAsync(
                            DeleteSnapshotsOption.None, 
                            AccessCondition.GenerateLeaseCondition(this.leaseId),
                            new BlobRequestOptions(),
                            null);   

                        this.logger.LogInformation("Control blob '{blob}' deleted", blobName);

                        this.logger.LogInformation("Finished");               
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

        private async Task NotifyJobFinished()
        {
            await this.jobFinishedNotifier.Notify(this.jobId, this.containerName, this.blobPrefix, this.cloudStorageAccount.Credentials.AccountName);
        }



        // Keeps the lease on the blob file as long as the cancellation token is not cancelled
        async Task RenewLease()
        {
            try
            {
                while (!this.cancellationTokenSource.IsCancellationRequested)
                {
                    await this.ctrlCloudBlob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(this.leaseId));

                    await Task.Delay(TimeSpan.FromSeconds(RENEW_LEASE_INTERVAL_IN_SECONDS), this.cancellationTokenSource.Token);

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

        async Task ProcessFile()
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
                            !this.cancellationTokenSource.IsCancellationRequested && 
                            linesRead < this.linesPerJob)
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
            this.cancellationTokenSource.Cancel();
            var tasksToWait = this.renewTask == null ? new Task[] { this.workerTask } : new Task[] { this.renewTask, this.workerTask };
            await Task.WhenAll(tasksToWait);
        }
    }
}
               