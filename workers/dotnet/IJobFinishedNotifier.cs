using System.Threading.Tasks;

namespace Worker
{
    public interface IJobFinishedNotifier 
    {
        Task Notify(string jobId, string containerName, string blobPrefix, string storageAcountName);
    }
}
               