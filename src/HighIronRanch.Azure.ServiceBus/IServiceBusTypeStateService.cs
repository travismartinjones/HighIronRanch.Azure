using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus
{
    public interface IServiceBusTypeStateService
    {
        Task<bool> GetIsQueueCreated(string name);
        Task OnQueueCreated(string name);
        Task<bool> GetIsTopicCreated(string name);
        Task OnTopicCreated(string name);
        Task<bool> GetIsSubscriptionCreated(string name);
        Task OnSubscriptionCreated(string name);
    }
}