namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IAggregateCommandHandler<T> : ICommandHandler<T> where T : IAggregateCommand
	{
	}
}