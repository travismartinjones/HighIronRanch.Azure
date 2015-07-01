using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;

namespace HighIronRanch.Azure.ServiceBus
{
	public class ServiceBusWithHandlersBuilder
	{
		private readonly IServiceBus _serviceBus;
		private readonly IHandlerActivator _handlerActivator;
		private readonly ILogger _logger;

		private IEnumerable<string> _messageAssembliesToScan;
		private IEnumerable<Type> _messageTypes; 

		private IEnumerable<string> _messageHandlerAssembliesToScan;
		private IEnumerable<Type> _messageHandlerTypes;

		private IEnumerable<string> _eventAssembliesToScan;
		private IEnumerable<Type> _eventTypes;

		private IEnumerable<string> _eventHandlerAssembliesToScan;
		private IEnumerable<Type> _eventHandlerTypes;

		public ServiceBusWithHandlersBuilder(IServiceBus serviceBus, IHandlerActivator handlerActivator, ILogger logger)
		{
			_serviceBus = serviceBus;
			_handlerActivator = handlerActivator;
			_logger = logger;
		}

		public ServiceBusWithHandlersBuilder CreateServiceBus()
		{
			return this;
		}

		public ServiceBusWithHandlersBuilder WithMessagesInAssembiles(IEnumerable<string> assembliesToScan)
		{
			_messageAssembliesToScan = assembliesToScan;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithMessages(IEnumerable<Type> commandTypes)
		{
			_messageTypes = commandTypes;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithMessageHandlersInAssemblies(IEnumerable<string> assembliesToScan)
		{
			_messageHandlerAssembliesToScan = assembliesToScan;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithMessageHandlers(IEnumerable<Type> handlerTypes)
		{
			_messageHandlerTypes = handlerTypes;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithEventsInAssembiles(IEnumerable<string> assembliesToScan)
		{
			_eventAssembliesToScan = assembliesToScan;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithEvents(IEnumerable<Type> commandTypes)
		{
			_eventTypes = commandTypes;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithEventHandlersInAssemblies(IEnumerable<string> assembliesToScan)
		{
			_eventHandlerAssembliesToScan = assembliesToScan;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithEventHandlers(IEnumerable<Type> handlerTypes)
		{
			_eventHandlerTypes = handlerTypes;
			return this;
		}

		public async Task<ServiceBusWithHandlers> BuildAsync()
		{
			var bus = new ServiceBusWithHandlers(_serviceBus, _handlerActivator, _logger);

			await CreateHandledQueuesInAssembliesAsync(bus);

			await CreateSpecificHandledQueuesAsync(bus);

			await CreateQueuesInAssembliesAsync(bus);

			await CreateSpecificQueuesAsync(bus);

			await CreateHandledEventsInAssembliesAsync(bus);

			await CreateSpecificHandledEventsAsync(bus);

			await CreateEventsInAssembliesAsync(bus);

			await CreateSpecificEventsAsync(bus);

			await bus.StartHandlers();

			return bus;
		}

		private async Task CreateSpecificQueuesAsync(ServiceBusWithHandlers bus)
		{
			if (_messageTypes != null)
			{
				await CreateQueuesAsync(_messageTypes, bus);
			}
		}

		private async Task CreateSpecificEventsAsync(ServiceBusWithHandlers bus)
		{
			if (_eventTypes != null)
			{
				await CreateEventsAsync(_eventTypes, bus);
			}
		}

		private async Task CreateQueuesInAssembliesAsync(ServiceBusWithHandlers bus)
		{
			if (_messageAssembliesToScan != null)
			{
				var assemblies = GetAssemblies(_messageAssembliesToScan);

				var commandTypesInAssemblies = assemblies
					.SelectMany(assembly => assembly.GetTypes())
					.Where(type => DoesTypeImplementInterface(type, typeof (IMessage)));

				await CreateQueuesAsync(commandTypesInAssemblies, bus);
			}
		}

		private async Task CreateEventsInAssembliesAsync(ServiceBusWithHandlers bus)
		{
			if (_eventAssembliesToScan != null)
			{
				var assemblies = GetAssemblies(_eventAssembliesToScan);

				var eventTypesInAssemblies = assemblies
					.SelectMany(assembly => assembly.GetTypes())
					.Where(type => DoesTypeImplementInterface(type, typeof (IEvent)));

				await CreateEventsAsync(eventTypesInAssemblies, bus);
			}
		}

		private async Task CreateSpecificHandledQueuesAsync(ServiceBusWithHandlers bus)
		{
			if (_messageHandlerTypes != null)
			{
				await CreateHandledQueuesAsync(_messageHandlerTypes, bus);
			}
		}

		private async Task CreateSpecificHandledEventsAsync(ServiceBusWithHandlers bus)
		{
			if (_eventHandlerTypes != null)
			{
				await CreateHandledEventsAsync(_eventHandlerTypes, bus);
			}
		}

		private IEnumerable<Assembly> GetAssemblies(IEnumerable<string> assemblies)
		{
			return AppDomain
				.CurrentDomain
				.GetAssemblies()
				.Where(assembly => assemblies.Contains(assembly.GetName().Name));
		} 

		private async Task CreateHandledEventsInAssembliesAsync(ServiceBusWithHandlers bus)
		{
			if (_eventHandlerAssembliesToScan != null)
			{
				var assemblies = GetAssemblies(_eventHandlerAssembliesToScan);

				var eventTypesInAssemblies = assemblies
					.SelectMany(assembly => assembly.GetTypes())
					.Where(type => DoesTypeImplementGenericInterface(type, typeof (IEventHandler<>)));

				await CreateHandledEventsAsync(eventTypesInAssemblies, bus);
			}
		}

		private async Task CreateHandledQueuesInAssembliesAsync(ServiceBusWithHandlers bus)
		{
			if (_messageHandlerAssembliesToScan != null)
			{
				var assemblies = GetAssemblies(_messageHandlerAssembliesToScan);

				var handlerTypesInAssemblies = assemblies
					.SelectMany(assembly => assembly.GetTypes())
					.Where(type => DoesTypeImplementGenericInterface(type, typeof (IMessageHandler<>)));

				await CreateHandledQueuesAsync(handlerTypesInAssemblies, bus);
			}
		}

		private static bool DoesTypeImplementGenericInterface(Type type, Type @interface)
		{
			if (!DoesTypeImplementInterface(type, @interface))
				return false;

			if (type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == @interface))
				return true;

			return false;
		}

		private static bool DoesTypeImplementInterface(Type type, Type @interface)
		{
			if (type.IsAbstract || type.IsInterface)
				return false;

			// This doesn't work: typeof(IMessageHandler<>).IsAssignableFrom(typeof(TestMessageHandler))
			//return @interface.IsAssignableFrom(type);

			// But this does:
			return type
				.GetInterfaces()
				.Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == @interface);
		}

		private async Task CreateQueuesAsync(IEnumerable<Type> commandTypes, ServiceBusWithHandlers bus)
		{
			foreach (var commandType in commandTypes)
			{
				await bus.CreateQueueAsync(commandType);
			}
		}

		private async Task CreateEventsAsync(IEnumerable<Type> eventTypes, ServiceBusWithHandlers bus)
		{
			foreach (var eventType in eventTypes)
			{
				await bus.CreateTopicAsync(eventType);
			}
		}

		private async Task CreateHandledQueuesAsync(IEnumerable<Type> handlerTypes, ServiceBusWithHandlers bus)
		{
			foreach (var handlerType in handlerTypes)
			{
				await bus.CreateHandledQueueAsync(handlerType);
			}
		}

		private async Task CreateHandledEventsAsync(IEnumerable<Type> eventTypes, ServiceBusWithHandlers bus)
		{
			foreach (var eventType in eventTypes)
			{
				await bus.CreateHandledEventAsync(eventType);
			}
		}
	}
}