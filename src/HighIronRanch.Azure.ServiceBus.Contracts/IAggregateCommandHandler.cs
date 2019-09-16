namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IAggregateCommandHandler<in T> : ICommandHandler<T> where T : IAggregateCommand
	{
	}
}