using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface ICommandActions
    {
        Task RenewLockAsync();
    }
}