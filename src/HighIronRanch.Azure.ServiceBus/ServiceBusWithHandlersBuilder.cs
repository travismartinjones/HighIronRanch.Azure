using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.ServiceBus;

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

		private bool _hasMultipleDeployments = true;
		private bool _useJsonSerialization = true;

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

		/// <summary>
		/// Default setting.
		/// Tells ServiceBus to only execute EventHandlers once across multiple deployments
		/// such as a webfarm. Json serialization is required when using multiple deployments.
		/// </summary>
		public ServiceBusWithHandlersBuilder WithMultipleDeployments()
		{
			if(!_useJsonSerialization)
				throw new ArgumentException("Json serialization is required when using multiple deployments.");

			_hasMultipleDeployments = true;
			return this;
		}

		/// <summary>
		/// Tells ServiceBus this application will be deployed in a standalone environment
		/// with no concern for redundancy. This simplifies the internals of EventHandling 
		/// a little bit. Json serialization is required when using multiple deployments.
		/// </summary>
		/// <returns></returns>
		public ServiceBusWithHandlersBuilder WithSingleDeployment()
		{
			_hasMultipleDeployments = false;
			return this;
		}

		/// <summary>
		/// Default setting.
		/// Serialize messages into json which is humanly readable in Service Bus Explorer. 
		/// Json serialization is required when using multiple deployments.
		/// </summary>
		public ServiceBusWithHandlersBuilder WithJsonSerialization()
		{
			_useJsonSerialization = true;
			return this;
		}

		/// <summary>
		/// Use the Azure Service Bus default object serialization for messages which are 
		/// not humanly readable in Service Bus Explorer. Json serialization is required 
		/// when using multiple deployments.
		/// </summary>
		public ServiceBusWithHandlersBuilder WithDefaultSerialization()
		{
			if (_hasMultipleDeployments)
				throw new ArgumentException("Json serialization is required when using multiple deployments.");

			_useJsonSerialization = false;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithCommandsInAssembiles(IEnumerable<string> assembliesToScan)
		{
			_messageAssembliesToScan = assembliesToScan;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithCommands(IEnumerable<Type> commandTypes)
		{
			_messageTypes = commandTypes;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithCommandHandlersInAssemblies(IEnumerable<string> assembliesToScan)
		{
			_messageHandlerAssembliesToScan = assembliesToScan;
			return this;
		}

		public ServiceBusWithHandlersBuilder WithCommandHandlers(IEnumerable<Type> handlerTypes)
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

		    ServiceBusEnvironment.SystemConnectivity.Mode = ConnectivityMode.Https;

			// set multiple deployments first because json serialization is true in ServiceBus by default
			// this will prevent any exception
			bus.HasMultipleDeployments(_hasMultipleDeployments);
			bus.UseJsonMessageSerialization(_useJsonSerialization);

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
					.Where(type => type.DoesTypeImplementInterface(typeof (ICommand)))
			        .ToList();

                var found = string.Join(",", commandTypesInAssemblies.Select(e => e.Name));
                _logger.Debug(ServiceBusWithHandlers.LoggerContext, "Found the following commands: {0}", found);

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
					.Where(type => type.DoesTypeImplementInterface(typeof (IEvent)))
			        .ToList();

                var found = string.Join(",", eventTypesInAssemblies.Select(e => e.Name));
                _logger.Debug(ServiceBusWithHandlers.LoggerContext, "Found the following events: {0}", found);

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

				var eventHandlerTypesInAssemblies = assemblies
					.SelectMany(assembly => assembly.GetTypes())
					.Where(type => type.DoesTypeImplementInterface(typeof(IEventHandler<>)))
			        .ToList();

                var found = string.Join(",", eventHandlerTypesInAssemblies.Select(e => e.Name));
                _logger.Debug(ServiceBusWithHandlers.LoggerContext, "Found the following event handlers: {0}", found);

                await CreateHandledEventsAsync(eventHandlerTypesInAssemblies, bus);
			}
		}

		private async Task CreateHandledQueuesInAssembliesAsync(ServiceBusWithHandlers bus)
		{
			if (_messageHandlerAssembliesToScan != null)
			{
				var assemblies = GetAssemblies(_messageHandlerAssembliesToScan);

				var handlerTypesInAssemblies = assemblies
					.SelectMany(assembly => assembly.GetTypes())
					.Where(type => type.DoesTypeImplementInterface(typeof(ICommandHandler<>)))
			        .ToList();

                var found = string.Join(",", handlerTypesInAssemblies.Select(e => e.Name));
                _logger.Debug(ServiceBusWithHandlers.LoggerContext, "Found the following command handlers: {0}", found);

                await CreateHandledQueuesAsync(handlerTypesInAssemblies, bus);
			}
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

		private async Task CreateHandledEventsAsync(IEnumerable<Type> eventHandlerTypes, ServiceBusWithHandlers bus)
		{
			foreach (var eventType in eventHandlerTypes)
			{
				await bus.CreateHandledEventAsync(eventType);
			}
		}
	}
}