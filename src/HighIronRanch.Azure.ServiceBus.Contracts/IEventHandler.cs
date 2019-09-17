using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IEventHandler<in T> where T : IEvent
	{
		Task HandleAsync(T evt, IEventActions eventActions);
	}
}