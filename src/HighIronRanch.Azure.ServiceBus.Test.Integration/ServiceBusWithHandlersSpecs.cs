using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using developwithpassion.specifications.rhinomocks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Machine.Specifications;

namespace HighIronRanch.Azure.ServiceBus.Test.Integration
{
	public class ServiceBusWithHandlersSpecs
	{
		public class TestCommand : ICommand
		{
			public string Content;
		}

		public class TestCommandHandler : ICommandHandler<TestCommand>
		{
			public static string LastHandledContent;

			public async Task HandleAsync(TestCommand command, ICommandActions actions)
			{
				LastHandledContent = command.Content;
			}
		}

		public class TestCommandLongHandler : ICommandHandler<TestCommand>
		{
			public static string LastHandledContent;

			public async Task HandleAsync(TestCommand command, ICommandActions actions)
			{
				Thread.Sleep(55000);
				await actions.RenewLockAsync();
				Thread.Sleep(10000);
				LastHandledContent = command.Content;
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

					case "TestCommandLongHandler":
						return new TestCommandLongHandler();
				}

				throw new ArgumentException("Unknown handler to activate: " + type.Name);
			}
		}

		[Subject(typeof(ServiceBusWithHandlers))]
		public class Concern : Observes<ServiceBusWithHandlers>
		{
			protected static IServiceBus _serviceBus;

			private Establish ee = () =>
			{
				var settings = ServiceBusSettings.Create();
				var nsManager = new NamespaceManagerBuilder();

				_serviceBus = new ServiceBus(settings, nsManager);

				depends.on(_serviceBus);
				depends.on(new HandlerActivator());
			};
		}

		public class CleaningConcern : Concern
		{
			private Cleanup cc = () =>
			{
				var task = _serviceBus.DeleteQueueAsync(typeof (TestCommand).FullName);
				task.Wait();
				//sut.Dispose();
			};
		}

		public class When_sending_a_message : CleaningConcern
		{
			private static string _testContent = Guid.NewGuid().ToString();

			private Establish context = () =>
			{
				var logger = fake.an<ILogger>();
				var activator = new HandlerActivator();

				sut_factory.create_using(() =>
				{
					var busBuilder = new ServiceBusWithHandlersBuilder(_serviceBus, activator, logger);

					busBuilder.CreateServiceBus()
						.WithMessageHandlers(new List<Type>() {typeof (TestCommandHandler)});
					var task = busBuilder.BuildAsync();
					task.Wait();
					return task.Result;
				});
			};

			private Because of = () =>
			{
				sut.SendAsync(new TestCommand() { Content = _testContent });
				// give a few seconds for message to come across
				var i = 30;
				do
				{
					Thread.Sleep(100);
					i--;
				} while (i > 0 && TestCommandHandler.LastHandledContent != _testContent);
			};

			private It should_be_handled = () => TestCommandHandler.LastHandledContent.ShouldEqual(_testContent);
		}

		public class When_sending_a_long_running_message : CleaningConcern
		{
			private static string _context = "Long running test";
			private static string _testContent = Guid.NewGuid().ToString();
			private static ILogger _logger = new TraceLogger();

			private Establish context = () =>
			{
				var activator = new HandlerActivator();

				sut_factory.create_using(() =>
				{
					var busBuilder = new ServiceBusWithHandlersBuilder(_serviceBus, activator, _logger);

					busBuilder.CreateServiceBus()
						.WithMessageHandlers(new List<Type>() { typeof(TestCommandLongHandler) });
					var task = busBuilder.BuildAsync();
					task.Wait();
					return task.Result;
				});
			};

			private Because of = () =>
			{
				sut.SendAsync(new TestCommand() { Content = _testContent });
				_logger.Information(_context, "Command sent");
				// The handler takes a while
				Thread.Sleep(59000);
				// start watching for message to come across
				var i = 15;
				do
				{
					Thread.Sleep(1000);
					i--;
					_logger.Information(_context, "Looking for content");
				} while (i > 0 && TestCommandLongHandler.LastHandledContent != _testContent);
				Thread.Sleep(100);
			};

			private It should_be_handled = () => TestCommandLongHandler.LastHandledContent.ShouldEqual(_testContent);
		}
	}
}