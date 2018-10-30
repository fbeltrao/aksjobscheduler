using System;

namespace Worker
{
    public class EventGridEvent<T> where T : class
    {
        public string Id { get; set; }
        public string Subject { get; set; }
        public string EventType { get; set; }
        public T Data { get; set; }
        public DateTime EventTime { get; set; }

        public EventGridEvent()
        {
            this.EventTime = DateTime.UtcNow;
        }
    }
}