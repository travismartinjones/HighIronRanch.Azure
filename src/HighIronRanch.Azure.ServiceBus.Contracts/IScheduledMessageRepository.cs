using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface IScheduledMessageRepository
    {
        Task Insert(string sessionId, string messageId, long sequenceId, string type, DateTime submitDate, DateTime scheduleEnqueueDate);
        Task Delete(string sessionId, string messageId);
        Task<ScheduledMessage> GetBySessionIdMessageId(string sessionId, string messageId);
        Task<List<ScheduledMessage>> GetBySessionIdType(string sessionId, string type);
        Task<List<ScheduledMessage>> GetBySessionIdTypeScheduledDateRange(string sessionId, string type, DateTime startDate, DateTime endDate);
    }
}