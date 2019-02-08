using System;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;

namespace HighIronRanch.Azure.ServiceBus
{
    public interface IServiceBusWithHandlers
    {
        void UseJsonMessageSerialization(bool useJsonSerialization);
        Task SendAsync(ICommand command, DateTime? enqueueTime = null);
        Task PublishAsync(IEvent evt);
        Task<long> GetMessageCount(Type type);
        Task<long> GetMessageCount(Type type, string sessionId);
        Task StartHandlers();
    }
}