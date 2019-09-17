using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface ICommandHandler<in T> where T : ICommand
	{
		Task HandleAsync(T message, ICommandActions actions);
	}
}