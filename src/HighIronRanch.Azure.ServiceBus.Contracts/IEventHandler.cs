using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IEventHandler<T> where T : IEvent
	{
		Task HandleAsync(T evt);
	}
}