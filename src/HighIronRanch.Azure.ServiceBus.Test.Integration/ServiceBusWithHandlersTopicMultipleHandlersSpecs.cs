using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using developwithpassion.specifications.rhinomocks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Azure.ServiceBus.Test.Common;
using HighIronRanch.Core.Services;
using Machine.Specifications;

namespace HighIronRanch.Azure.ServiceBus.Test.Integration
{
	public class ServiceBusWithHandlersTopicMultipleHandlersSpecs
	{
		public class TestEvent : IEvent
		{
			public string Content;
		}

		public class TestEventHandler : IEventHandler<TestEvent>
		{
			public static string LastHandledContent;

			public async Task HandleAsync(TestEvent evt)
			{
				LastHandledContent = evt.Content;
			}
		}

		public class SecondTestEventHandler : IEventHandler<TestEvent>
		{
			public static string LastHandledContent;

			public async Task HandleAsync(TestEvent evt)
			{
				LastHandledContent = evt.Content;
			}
		}

		public class HandlerActivator : IHandlerActivator
		{
			public object GetInstance(Type type)
			{
				switch (type.Name)
				{
					case "TestEventHandler":
						return new TestEventHandler();

					case "SecondTestEventHandler":
						return new SecondTestEventHandler();
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
			};
		}

		public class CleaningConcern : Concern
		{
			private Cleanup cc = () =>
			{
				var task = _serviceBus.DeleteTopicAsync(typeof(TestEvent).FullName);
				task.Wait();
				task = _serviceBus.DeleteQueueAsync(typeof(TestEvent).FullName);
				task.Wait();
				//sut.Dispose();
			};
		}

		public class When_sending_an_event_with_two_handlers : CleaningConcern
		{
			private static readonly string _testContent = Guid.NewGuid().ToString();

			private Establish context = () =>
			{
				var logger = fake.an<ILogger>();
				var activator = new HandlerActivator();
                var scheduledMessageRepository = fake.an<IScheduledMessageRepository>();

				sut_factory.create_using(() =>
				{
					var busBuilder = new ServiceBusWithHandlersBuilder(_serviceBus, activator, logger, scheduledMessageRepository);

					busBuilder.CreateServiceBus()
						.WithEventHandlers(new List<Type>() { typeof(TestEventHandler), typeof(SecondTestEventHandler) });
					var task = busBuilder.BuildAsync();
					task.Wait();
					return task.Result;
				});
			};

			private Because of = () =>
			{
				sut.PublishAsync(new TestEvent() { Content = _testContent }).Wait();
				// give a few seconds for message to come across
				var i = 30;
				do
				{
					Thread.Sleep(100);
					i--;
				} while (i > 0 && TestEventHandler.LastHandledContent != _testContent);
			};

			private It should_be_handled_by_both_handlers = () =>
			{
				TestEventHandler.LastHandledContent.ShouldEqual(_testContent);
				SecondTestEventHandler.LastHandledContent.ShouldEqual(_testContent);
			};
		}
	}
}