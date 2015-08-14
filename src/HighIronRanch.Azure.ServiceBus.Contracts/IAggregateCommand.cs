using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IAggregateCommand : ICommand
	{
		Guid GetAggregateId();
	}
}