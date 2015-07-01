using System;
//using HighIronRanch.IoC;

namespace HighIronRanch.Azure.ServiceBus.Contracts
{
	public interface IHandlerActivator
	{
		object GetInstance(Type type);
	}

/*
	public class HandlerActivator : IHandlerActivator
	{
		public object GetInstance(Type type)
		{
			return Container.GetInstance(type);
		}
	}
*/
}