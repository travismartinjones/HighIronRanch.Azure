using System;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using Microsoft.Azure.ServiceBus;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class EventActions : IEventActions
    {
        private readonly Func<Task> _renewAction;
        private readonly Message _message;

        public EventActions(Func<Task> renewAction, Message message, CancellationToken token)
        {
            _renewAction = renewAction;
            _message = message;
            CancellationToken = token;
        }

        public async Task RenewLockAsync()
        {
            await _renewAction.Invoke().ConfigureAwait(false);
        }

        public int DeliveryCount => _message.SystemProperties.DeliveryCount;
        public bool IsLastDelivery => _message.SystemProperties.DeliveryCount >= 9;
        public CancellationToken CancellationToken { get; }
    }
}