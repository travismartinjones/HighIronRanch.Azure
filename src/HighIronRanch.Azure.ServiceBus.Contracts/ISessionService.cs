using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface ISessionService
    {        
        void Add(MessageSession session);
        void Remove(MessageSession session);
        Task ClearAll();
    }
}