using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface ICommand : IMessage
	{
		Guid GetSessionId();
	}
}