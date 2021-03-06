﻿using System.Threading;
using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface ICommandActions
    {
        Task RenewLockAsync();
        int DeliveryCount { get; }
        bool IsLastDelivery { get; }
        CancellationToken CancellationToken { get; }
    }
}