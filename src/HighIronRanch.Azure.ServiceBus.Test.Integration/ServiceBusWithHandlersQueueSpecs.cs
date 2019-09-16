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
	public class ServiceBusWithHandlersQueueSpecs
	{
		public class TestCommand : ICommand
		{
			public string Content;
		    public Guid MessageId => Guid.NewGuid();
		}

        public class TestCommandHandler : ICommandHandler<TestCommand>
		{
			public static string LastHandledContent;

			public async Task HandleAsync(TestCommand command, ICommandActions actions)
			{
				LastHandledContent = command.Content;
			}
		}

        public class TestAggCommand : IAggregateCommand
        {
            public string Content;
            public string AggregateId;
            public Guid MessageId => Guid.NewGuid();

            public string GetAggregateId()
            {
                return AggregateId;
            }
        }

        public class TestAggCommandHandler : IAggregateCommandHandler<TestAggCommand>
        {
            public static string LastHandledContent;

            public async Task HandleAsync(TestAggCommand command, ICommandActions actions)
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
				await actions.RenewLockAsync().ConfigureAwait(false);
				Thread.Sleep(10000);
				LastHandledContent = command.Content;
			}
		}

		public class HandlerActivator : IHandlerActivator
		{
		    private readonly ILogger _logger;

		    public HandlerActivator(ILogger logger)
		    {
		        _logger = logger;
		    }

		    public object GetInstance(Type type)
		    {
		        _logger.Debug("HandlerActivator", "Getting Instance of {0}", type.Name);

				switch (type.Name)
				{
					case "TestCommandHandler":
						return new TestCommandHandler();

                    case "TestAggCommandHandler":
                        return new TestAggCommandHandler();

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
		    protected static ILogger _logger;

			private Establish ee = () =>
			{
                _logger = new TraceLogger();

				var settings = ServiceBusSettings.Create();
				var nsManager = new NamespaceManagerBuilder();

				_serviceBus = new ServiceBus(settings, nsManager);

				depends.on(_serviceBus);
				depends.on(new HandlerActivator(_logger));
			};
		}

		public class CleaningConcern : Concern
		{
			private Cleanup cc = () =>
			{
                sut.Dispose();
			    //Thread.Sleep(2000);
                var task = _serviceBus.DeleteQueueAsync(typeof (TestCommand).FullName);
				task.Wait();
                task = _serviceBus.DeleteQueueAsync(typeof(TestAggCommand).FullName);
                task.Wait();
            };
		}

		public class When_sending_a_command : CleaningConcern
		{
			private static readonly string _testContent = Guid.NewGuid().ToString();

			private Establish context = () =>
			{
				var activator = new HandlerActivator(_logger);

				sut_factory.create_using(() =>
				{
					var busBuilder = new ServiceBusWithHandlersBuilder(_serviceBus, activator, _logger);

					busBuilder.CreateServiceBus()
						.WithCommandHandlers(new List<Type>() {typeof (TestCommandHandler)});
					var task = busBuilder.BuildAsync();
					task.Wait();
					var bus = task.Result;
					return bus;
				});
			};

			private Because of = () =>
			{
				sut.SendAsync(new TestCommand() { Content = _testContent }).Wait();
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

        public class When_sending_an_aggregate_command : CleaningConcern
        {
            private static readonly string _testContent = Guid.NewGuid().ToString();

            private Establish context = () =>
            {
                var activator = new HandlerActivator(_logger);

                sut_factory.create_using(() =>
                {
                    var busBuilder = new ServiceBusWithHandlersBuilder(_serviceBus, activator, _logger);

                    busBuilder.CreateServiceBus()
                        .WithCommandHandlers(new List<Type>() { typeof(TestAggCommandHandler) });
                    var task = busBuilder.BuildAsync();
                    task.Wait();
                    var bus = task.Result;
                    return bus;
                });
            };

            private Because of = () =>
            {
                sut.SendAsync(new TestAggCommand() { Content = _testContent, AggregateId = "AggId"}).Wait();

                // give a few seconds for message to come across
                var i = 30;
                do
                {
                    Thread.Sleep(100);
                    i--;
                } while (i > 0 && TestAggCommandHandler.LastHandledContent != _testContent);
            };

            private It should_be_handled = () => TestAggCommandHandler.LastHandledContent.ShouldEqual(_testContent);
        }

        public class When_sending_a_long_running_command : CleaningConcern
		{
			private static string _context = "Long running test";
			private static readonly string _testContent = Guid.NewGuid().ToString();

			private Establish context = () =>
			{
				var activator = new HandlerActivator(_logger);

				sut_factory.create_using(() =>
				{
					var busBuilder = new ServiceBusWithHandlersBuilder(_serviceBus, activator, _logger);

					busBuilder.CreateServiceBus()
						.WithCommandHandlers(new List<Type>() { typeof(TestCommandLongHandler) });
					var task = busBuilder.BuildAsync();
					task.Wait();
					return task.Result;
				});
			};

			private Because of = () =>
			{
				sut.SendAsync(new TestCommand() { Content = _testContent }).Wait();
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