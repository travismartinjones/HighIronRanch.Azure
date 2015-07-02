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
		public class TestMessage : IMessage
		{
			public string Content;
		}

		public class TestMessageHandler : IMessageHandler<TestMessage>
		{
			public static string LastHandledContent;

			public Task HandleAsync(TestMessage message)
			{
				LastHandledContent = message.Content;
				return Task.FromResult(0);
			}
		}

		public class HandlerActivator : IHandlerActivator
		{
			public object GetInstance(Type type)
			{
				switch (type.Name)
				{
					case "TestMessageHandler":
						return new TestMessageHandler();
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
				_serviceBus.DeleteQueueAsync(typeof (TestMessage).FullName);
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
						.WithMessageHandlers(new List<Type>() {typeof (TestMessageHandler)});
					var task = busBuilder.BuildAsync();
					task.Wait();
					return task.Result;
				});
			};

			private Because of = () =>
			{
				sut.SendAsync(new TestMessage() { Content = _testContent });
				// give a few seconds for message to come across
				var i = 30;
				do
				{
					Thread.Sleep(100);
					i--;
				} while (i > 0 && TestMessageHandler.LastHandledContent != _testContent);
				Thread.Sleep(100);
			};

			private It should_be_handled = () => TestMessageHandler.LastHandledContent.ShouldEqual(_testContent);
		}
	}
}