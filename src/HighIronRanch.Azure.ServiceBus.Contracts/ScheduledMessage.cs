using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public class ScheduledMessage
    {
        public string SessionId { get; set; }
        public string CorrelationId { get;set; }
        public long SequenceId { get; set; }
        public string Type { get; set; }
        public DateTime SubmitDate { get; set; }
        public DateTime ScheduleEnqueueDate { get; set; }
        public bool IsCancelled { get; set; }
    }
}