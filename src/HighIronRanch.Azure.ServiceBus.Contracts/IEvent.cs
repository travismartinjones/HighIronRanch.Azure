using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IEvent
	{
        // a unique id to prevent duplicate publication of the same event
        Guid MessageId { get; }
	}

    public interface IAggregateEvent : IEvent
    {
        string GetAggregateId();
    }
}