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
#if USE_MESSAGING_FACTORY
            var factoryBuilder = new MessagingFactoryBuilder();
#endif

            var serviceBus = new ServiceBus(settings, nsManager
#if USE_MESSAGING_FACTORY
                , factoryBuilder
#endif
                );

            Logger = new Logger();
			var activator = new HandlerActivator();

			var busBuilder = new ServiceBusWithHandlersBuilder(serviceBus, activator, Logger);

			Logger.Information("Main", "Building bus");
			busBuilder.CreateServiceBus()
				.WithCommandHandlers(new List<Type>() { typeof(TestCommandHandler), typeof(TestAggCommandHandler) });
			var task = busBuilder.BuildAsync();
			task.Wait();
			var bus = task.Result;

			Logger.Information("Main", "Ready. Press 'p' to publish a command. Press 'a' to publish an aggregate command. Press 'q' to quit.");

			while (true)
			{
				var key = Console.ReadKey(true);
				if (key.KeyChar == 'q')
					break;
				if (key.KeyChar == 'p')
				{
					var testContent = Guid.NewGuid().ToString();

					Logger.Information("Main", "Publishing command for {0}", testContent);
					bus.SendAsync(new TestCommand() { Content = testContent }).Wait();
					Logger.Information("Main", "Published");
				}
                else if (key.KeyChar == 'a')
                {
                    var testContent = Guid.NewGuid().ToString();

                    Logger.Information("Main", "Publishing agg command for {0}", testContent);
                    bus.SendAsync(new TestAggCommand() { Content = testContent }).Wait();
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

    public class TestAggCommand : IAggregateCommand
    {
        public string Content;
        public string GetAggregateId()
        {
            return Content;
        }
    }

    public class TestAggCommandHandler : IAggregateCommandHandler<TestAggCommand>
    {
        public async Task HandleAsync(TestAggCommand message, ICommandActions actions)
        {
            Program.Logger.Information("TestAggCommandHandler", "Handling Agg: {0}", message.Content);
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

                case "TestAggCommandHandler":
                    return new TestAggCommandHandler();
            }

            throw new ArgumentException("Unknown handler to activate: " + type.Name);
		}
	}
}
