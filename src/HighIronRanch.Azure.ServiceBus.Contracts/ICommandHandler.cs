using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface ICommandHandler<T> where T : ICommand
	{
		Task HandleAsync(T message, ICommandActions actions);
	}
}