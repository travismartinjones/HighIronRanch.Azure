﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using Microsoft.Azure.ServiceBus;

namespace HighIronRanch.Azure.ServiceBus.Standard
{
    public class CommandActions : ICommandActions
    {
        private readonly Func<Task> _renewLock;
        private readonly Message _message;

        public CommandActions(Func<Task> renewLock, Message message, CancellationToken token)
        {
            _renewLock = renewLock;
            _message = message;
            CancellationToken = token;
        }

        public async Task RenewLockAsync()
        {
            await _renewLock.Invoke().ConfigureAwait(false);
        }

        
        public int DeliveryCount => _message.SystemProperties.DeliveryCount;
        public bool IsLastDelivery => _message.SystemProperties.DeliveryCount >= 9;
        public CancellationToken CancellationToken { get; }
    }
}
