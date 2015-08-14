using System;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IHandlerActivator
	{
		object GetInstance(Type type);
	}
}