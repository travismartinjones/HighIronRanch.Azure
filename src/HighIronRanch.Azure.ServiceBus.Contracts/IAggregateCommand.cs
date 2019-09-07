using System;
using System.Collections.Generic;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface IAggregateCommand : ICommand
	{
        Guid MessageId { get; }
        string GetAggregateId();
	}
}