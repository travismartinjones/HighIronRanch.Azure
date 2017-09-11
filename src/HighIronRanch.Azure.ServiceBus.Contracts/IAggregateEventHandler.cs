namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface IAggregateEventHandler<T> : IEventHandler<T> where T : IAggregateEvent
    {		
    }
}