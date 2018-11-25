using System;
using System.Threading.Tasks;

namespace BatchConnector
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var jobId = Environment.GetEnvironmentVariable("JOBID");
            var poolId = Environment.GetEnvironmentVariable("POOLID");
            var imageName = Environment.GetEnvironmentVariable("WORKERIMAGENAME");
            var storageAccount = Environment.GetEnvironmentVariable("STORAGEACCOUNT");
            var storageKey = Environment.GetEnvironmentVariable("STORAGEKEY");
            var blobPrefix = Environment.GetEnvironmentVariable("BLOBPREFIX");
            var container = Environment.GetEnvironmentVariable("CONTAINER");
            var aggregatorImageName = Environment.GetEnvironmentVariable("AGGREGATORIMAGENAME");
            var completions = int.Parse(Environment.GetEnvironmentVariable("COMPLETIONS"));
            Console.WriteLine("Environment variables parsed, starting job submission...");
            await BatchCommunication.SubmitJobAsync(jobId, poolId, imageName, completions, storageAccount, storageKey, blobPrefix, container, aggregatorImageName);
        }
    }
}
