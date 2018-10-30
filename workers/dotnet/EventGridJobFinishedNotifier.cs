using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Worker
{
    public class EventGridJobFinishedNotifier : IJobFinishedNotifier
    {
        private readonly ILogger logger;
        private string eventGridTopicEndpoint;
        private string eventGridSasKey;

        public EventGridJobFinishedNotifier(IConfiguration configuration, ILogger<EventGridJobFinishedNotifier> logger)
        {
            this.eventGridTopicEndpoint = configuration.GetValue<string>("EVENTGRIDTOPICENDPOINT");
            this.eventGridSasKey = configuration.GetValue<string>("EVENTGRIDSASKEY");
            this.logger = logger;

            this.logger.LogInformation("Event grid topic endpoint: {eventGridTopicEndpoint}", this.eventGridTopicEndpoint);
            this.logger.LogInformation("Event grid sas key: {eventGridSasKey}", this.eventGridSasKey);
        }

        public async Task Notify(string jobId, string containerName, string blobPrefix, string storageAcountName)
        {
            try
            {
                // ok to create one here since we only send it once
                // refactor to a factory if we need multiple notifications
                var client = new HttpClient();
                client.DefaultRequestHeaders.Add("aeg-sas-key", this.eventGridSasKey);
                client.DefaultRequestHeaders.UserAgent.ParseAdd("aksjobscheduler");

                var egEvent = new EventGridEvent<JobFinishedEvent>
                {
                    Data = new JobFinishedEvent
                    {
                        ContainerName = containerName,
                        JobId = jobId,
                        BlobPrefix = blobPrefix,
                        StorageAccountName = storageAcountName,

                    },
                    EventType = "job finished",
                    Subject = jobId,
                    Id = Guid.NewGuid().ToString(),
                };

                var jsonPayload = JsonConvert.SerializeObject(new EventGridEvent<JobFinishedEvent>[] { egEvent });
                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, this.eventGridTopicEndpoint)
                {
                    Content = new StringContent(jsonPayload, Encoding.UTF8, "application/json")
                };

                HttpResponseMessage response = await client.SendAsync(request);
                if (response.IsSuccessStatusCode)
                {
                    logger.LogInformation("Notification to event grid succeeded: {payload}", jsonPayload);
                }
                else
                {
                    logger.LogError("Notification to event grid failed with status {status} and message '{message}': {payload}", response.StatusCode, await response.Content.ReadAsStringAsync(), jsonPayload);
                }

            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Failed to notify using event grid");
            }
        }
    }
}