using System.Threading;
using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface IEventActions
    {
        Task RenewLockAsync();
        int DeliveryCount { get; }
        bool IsLastDelivery { get; }
        CancellationToken CancellationToken { get; }
    }
}