using System;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;

namespace HighIronRanch.Azure.ServiceBus
{
    public interface IServiceBusWithHandlers
    {        
        Task SendAsync(ICommand command, DateTime? enqueueTime = null);
        Task SendAsync(ICommand command, EnqueueOptions options);
        Task PublishAsync(IEvent evt);
        Task StartHandlers();
    }
}