namespace HighIronRanch.Azure.ServiceBus.Contracts
{
    public interface IAggregateEventHandler<in T> : IEventHandler<T> where T : IAggregateEvent
    {		
    }
}