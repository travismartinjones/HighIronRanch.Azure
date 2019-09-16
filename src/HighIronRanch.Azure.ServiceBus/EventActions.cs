using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class EventActions : IEventActions
    {
        private readonly BrokeredMessage _message;

        public EventActions(BrokeredMessage message, CancellationToken token)
        {
            _message = message;
            CancellationToken = token;
        }

        public async Task RenewLockAsync()
        {
            await _message.RenewLockAsync().ConfigureAwait(false);
        }

        public int DeliveryCount => _message.DeliveryCount;
        public bool IsLastDelivery => _message.DeliveryCount >= 9;
        public CancellationToken CancellationToken { get; }
    }
}