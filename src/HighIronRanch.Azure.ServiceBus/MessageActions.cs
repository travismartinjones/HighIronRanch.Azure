using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
	internal class MessageActions : IMessageActions
	{
		private readonly BrokeredMessage _message;

		public MessageActions(BrokeredMessage message)
		{
			_message = message;
		}

		public async Task RenewLockAsync()
		{
			await _message.RenewLockAsync();
		}
	}
}