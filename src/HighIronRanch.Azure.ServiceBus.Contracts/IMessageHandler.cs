using System.Threading.Tasks;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IMessageHandler<T> where T : IMessage
	{
		Task HandleAsync(T message, IMessageActions actions);
	}
}