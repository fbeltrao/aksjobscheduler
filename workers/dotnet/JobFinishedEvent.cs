namespace Worker
{
    public class JobFinishedEvent 
    {
        public string JobId { get; set; }
        public string ContainerName { get; set; }
        public string StorageAccountName { get; set; }
        public string BlobPrefix { get; set; }
    }
}