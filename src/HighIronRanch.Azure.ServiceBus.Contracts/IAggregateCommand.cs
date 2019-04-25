using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface IAggregateCommand : ICommand
	{
        Guid MessageId { get; }
        string GetAggregateId();
	}
}