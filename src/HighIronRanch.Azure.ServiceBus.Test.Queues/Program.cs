using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Azure.ServiceBus.Test.Common;
using HighIronRanch.Core.Services;

namespace HighIronRanch.Azure.ServiceBus.Test.Queues
{
	class Program
	{
		public static ILogger Logger;

		static void Main(string[] args)
		{
			var settings = ServiceBusSettings.Create();
			settings.ServiceBusSubscriptionNamePrefix = DateTime.Now.ToString("hhmmss");

			var nsManager = new NamespaceManagerBuilder();

			var serviceBus = new ServiceBus(settings, nsManager);

			Logger = new Logger();
			var activator = new HandlerActivator();

			var busBuilder = new ServiceBusWithHandlersBuilder(serviceBus, activator, Logger);

			Logger.Information("Main", "Building bus");
			busBuilder.CreateServiceBus()
				.WithCommandHandlers(new List<Type>() { typeof(TestCommandHandler) });
			var task = busBuilder.BuildAsync();
			task.Wait();
			var bus = task.Result;

			Logger.Information("Main", "Ready. Press 'p' to publish an event. Press 'q' to quit.");

			while (true)
			{
				var key = Console.ReadKey(true);
				if (key.KeyChar == 'q')
					break;
				if (key.KeyChar == 'p')
				{
					var testContent = Guid.NewGuid().ToString();

					Logger.Information("Main", "Publishing event for {0}", testContent);
					bus.SendAsync(new TestCommand() { Content = testContent }).Wait();
					Logger.Information("Main", "Published");
				}

				Thread.Sleep(100);
			}

			Logger.Information("Main", "Cleanup");
			var cleanupTask = serviceBus.DeleteQueueAsync(typeof(TestCommand).FullName);
			cleanupTask.Wait();
		}
	}

	public class TestCommand : ICommand
	{
		public string Content;
	}

	public class TestCommandHandler : ICommandHandler<TestCommand>
	{
		public async Task HandleAsync(TestCommand message, ICommandActions actions)
		{
			Program.Logger.Information("TestCommandHandler", "Handling: {0}", message.Content);
		}
	}

	public class HandlerActivator : IHandlerActivator
	{
		public object GetInstance(Type type)
		{
			switch (type.Name)
			{
				case "TestCommandHandler":
					return new TestCommandHandler();
			}

			throw new ArgumentException("Unknown handler to activate: " + type.Name);
		}
	}
}
