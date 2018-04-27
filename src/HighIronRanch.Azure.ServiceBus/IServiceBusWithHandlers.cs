using System;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;

namespace HighIronRanch.Azure.ServiceBus
{
    public interface IServiceBusWithHandlers : IDisposable
    {
        void UseJsonMessageSerialization(bool useJsonSerialization);
        Task SendAsync(ICommand command, DateTime? enqueueTime = null);
        Task PublishAsync(IEvent evt);
        Task<long> GetMessageCount(Type type);
        Task StartHandlers();
    }
}