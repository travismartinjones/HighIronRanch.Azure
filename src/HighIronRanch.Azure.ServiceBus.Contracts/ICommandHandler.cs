using HighIronRanch.Azure.ServiceBus.Contracts;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface ICommandHandler<T> : IMessageHandler<T> where T : ICommand
	{
	}
}