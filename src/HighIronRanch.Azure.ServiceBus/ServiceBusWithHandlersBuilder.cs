using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;

namespace HighIronRanch.Azure.ServiceBus
{
	public class ServiceBusWithHandlersBuilder
	{
		private readonly IServiceBus _serviceBus;
		private readonly IHandlerActivator _handlerActivator;
		private readonly ILogger _logger;
        private readonly IScheduledMessageRepository _scheduledMessageRepository;
        private readonly IServiceBusSettings _serviceBusSettings;

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
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly int _maxConcurrentSessions;
        private readonly int _defaultWaitSeconds;
        private readonly int _autoRenewMultiplier;

        public ServiceBusWithHandlersBuilder(
            IServiceBus serviceBus, 
            IHandlerActivator handlerActivator, 
            ILogger logger, 
            IScheduledMessageRepository scheduledMessageRepository, 
            IHandlerStatusProcessor handlerStatusProcessor,
            IServiceBusSettings serviceBusSettings,
            int maxConcurrentSessions,
            int defaultWaitSeconds,
            int autoRenewMultiplier)
        {
            _serviceBus = serviceBus;
            _handlerActivator = handlerActivator;
            _logger = logger;
            _scheduledMessageRepository = scheduledMessageRepository;
            _handlerStatusProcessor = handlerStatusProcessor;
            _serviceBusSettings = serviceBusSettings;
            _maxConcurrentSessions = maxConcurrentSessions;
            _defaultWaitSeconds = defaultWaitSeconds;
            _autoRenewMultiplier = autoRenewMultiplier;
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
			var bus = new ServiceBusWithHandlers(_serviceBus, _handlerActivator, _logger, _handlerStatusProcessor, _scheduledMessageRepository, _maxConcurrentSessions, _defaultWaitSeconds, _autoRenewMultiplier);
		    
            var connection = new ServiceBusConnection(_serviceBusSettings.AzureServiceBusConnectionString);

			await CreateHandledQueuesInAssembliesAsync(connection, bus).ConfigureAwait(false);

			await CreateSpecificHandledQueuesAsync(connection, bus).ConfigureAwait(false);

			await CreateQueuesInAssembliesAsync(connection, bus).ConfigureAwait(false);

			await CreateSpecificQueuesAsync(connection, bus).ConfigureAwait(false);

			await CreateHandledEventsInAssembliesAsync(connection, bus).ConfigureAwait(false);

			await CreateSpecificHandledEventsAsync(connection, bus).ConfigureAwait(false);

			await CreateEventsInAssembliesAsync(connection, bus).ConfigureAwait(false);

			await CreateSpecificEventsAsync(connection, bus).ConfigureAwait(false);

		    await bus.StartHandlers(connection).ConfigureAwait(false);

			return bus;
		}

		private async Task CreateSpecificQueuesAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
		{
			if (_messageTypes != null)
			{
				await CreateQueuesAsync(connection, _messageTypes, bus).ConfigureAwait(false);
			}
		}

		private async Task CreateSpecificEventsAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
		{
			if (_eventTypes != null)
			{
				await CreateEventsAsync(connection, _eventTypes, bus).ConfigureAwait(false);
			}
		}

		private async Task CreateQueuesInAssembliesAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
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

                await CreateQueuesAsync(connection, commandTypesInAssemblies, bus).ConfigureAwait(false);
			}
		}

		private async Task CreateEventsInAssembliesAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
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

				await CreateEventsAsync(connection, eventTypesInAssemblies, bus).ConfigureAwait(false);
			}
		}

		private async Task CreateSpecificHandledQueuesAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
		{
			if (_messageHandlerTypes != null)
			{
				await CreateHandledQueuesAsync(connection, _messageHandlerTypes, bus).ConfigureAwait(false);
			}
		}

		private async Task CreateSpecificHandledEventsAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
		{
			if (_eventHandlerTypes != null)
			{
				await CreateHandledEventsAsync(connection, _eventHandlerTypes, bus).ConfigureAwait(false);
			}
		}

		private IEnumerable<Assembly> GetAssemblies(IEnumerable<string> assemblies)
		{
			return AppDomain
				.CurrentDomain
				.GetAssemblies()
				.Where(assembly => assemblies.Contains(assembly.GetName().Name));
		} 

		private async Task CreateHandledEventsInAssembliesAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
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

                await CreateHandledEventsAsync(connection, eventHandlerTypesInAssemblies, bus).ConfigureAwait(false);
			}
		}

		private async Task CreateHandledQueuesInAssembliesAsync(ServiceBusConnection connection, ServiceBusWithHandlers bus)
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

                await CreateHandledQueuesAsync(connection, handlerTypesInAssemblies, bus).ConfigureAwait(false);
			}
		}

		private async Task CreateQueuesAsync(ServiceBusConnection connection, IEnumerable<Type> commandTypes, ServiceBusWithHandlers bus)
		{
			foreach (var commandType in commandTypes)
			{
				await bus.CreateQueueAsync(connection, commandType).ConfigureAwait(false);
			}
		}

		private async Task CreateEventsAsync(ServiceBusConnection connection, IEnumerable<Type> eventTypes, ServiceBusWithHandlers bus)
		{
			foreach (var eventType in eventTypes)
			{
				await bus.CreateTopicAsync(connection, eventType).ConfigureAwait(false);
			}
		}

		private async Task CreateHandledQueuesAsync(ServiceBusConnection connection, IEnumerable<Type> handlerTypes, ServiceBusWithHandlers bus)
		{
			foreach (var handlerType in handlerTypes)
			{
				await bus.CreateHandledQueueAsync(connection, handlerType).ConfigureAwait(false);
			}
		}

		private async Task CreateHandledEventsAsync(ServiceBusConnection connection, IEnumerable<Type> eventHandlerTypes, ServiceBusWithHandlers bus)
		{
			foreach (var eventType in eventHandlerTypes)
			{
				await bus.CreateHandledEventAsync(connection, eventType).ConfigureAwait(false);
			}
		}
	}
}