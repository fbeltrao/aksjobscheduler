using System;

namespace BatchConnector
{
    class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            String jobId = Environment.GetEnvironmentVariable("JOBID");
            String poolId = Environment.GetEnvironmentVariable("POOLID");
            String imageName = Environment.GetEnvironmentVariable("WORKERIMAGENAME");
            String storageAccount = Environment.GetEnvironmentVariable("STORAGEACCOUNT");
            String storageKey = Environment.GetEnvironmentVariable("STORAGEKEY");
            String blobPrefix = Environment.GetEnvironmentVariable("BLOBPREFIX");
            String container = Environment.GetEnvironmentVariable("CONTAINER");
            String aggregatorImageName = Environment.GetEnvironmentVariable("AGGREGATORIMAGENAME");
            int completions = int.Parse(Environment.GetEnvironmentVariable("COMPLETIONS"));
            System.Console.WriteLine("Environment variables parsed, starting job submission...");
            await BatchCommunication.SubmitJobAsync(jobId, poolId, imageName, completions, storageAccount, storageKey, blobPrefix, container, aggregatorImageName);
        }
    }
}
