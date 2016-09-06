using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface ICommand
	{
        // a unique id to prevent duplicate publication of the same command
        Guid MessageId { get; }
    }
}