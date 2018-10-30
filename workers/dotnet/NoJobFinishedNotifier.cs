using System.Threading.Tasks;

namespace Worker
{
    public class NoJobFinishedNotifier : IJobFinishedNotifier
    {
        public Task Notify(string jobId, string containerName, string blobPrefix, string storageAcountName) => Task.FromResult(0);
    }
}