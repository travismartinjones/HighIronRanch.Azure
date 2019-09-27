using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.InteropExtensions;
using Newtonsoft.Json;
using ExceptionReceivedEventArgs = Microsoft.Azure.ServiceBus.ExceptionReceivedEventArgs;
using QueueClient = Microsoft.Azure.ServiceBus.QueueClient;
using SessionHandlerOptions = Microsoft.Azure.ServiceBus.SessionHandlerOptions;
using TopicClient = Microsoft.Azure.ServiceBus.TopicClient;

namespace HighIronRanch.Azure.ServiceBus
{
    public class ServiceBusWithHandlers : IServiceBusWithHandlers
	{
		public static readonly string LoggerContext = "HighIronRanch.Azure.ServiceBus";

        // don't increase MessagePrefetchSize to something like 100, it will cause messages to deadletter, this is due to the lock window
        // being exceeded, and causing message delivery counts to increase, even though they haven't been processed
        // in this github example by microsoft, they changed their initial 100 (see comments) to 10, likely due to the same issue
        // https://github.com/Azure/azure-service-bus/blob/master/samples/DotNet/Microsoft.ServiceBus.Messaging/Prefetch/Program.cs
	    private static int MessagePrefetchSize = 10;
		private readonly IServiceBus _serviceBus;
		private bool _hasMultipleDeployments = true;
		private bool _useJsonSerialization = true;
		private readonly IHandlerActivator _handlerActivator;
		private readonly ILogger _logger;
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly IScheduledMessageRepository _scheduledMessageRepository;
        private readonly int _maxConcurrentSessions;
        private readonly int _defaultWaitSeconds;
        private readonly int _autoRenewMultiplier;
        protected CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        
		// ICommand, QueueClient
		protected readonly IDictionary<Type, QueueClient> _queueClients = new ConcurrentDictionary<Type, QueueClient>();

		// ICommand, ICommandHandler
		protected readonly IDictionary<Type, Type> _queueHandlers = new ConcurrentDictionary<Type, Type>();

		// IEvent, TopicClient
		protected readonly IDictionary<Type, TopicClient> _topicClients = new ConcurrentDictionary<Type, TopicClient>();
 
		// IEvent, ISet<IEventHandler>
		protected readonly IDictionary<Type, ISet<Type>> _eventHandlers = new ConcurrentDictionary<Type, ISet<Type>>();	    

	    public ServiceBusWithHandlers(
            IServiceBus serviceBus, 
            IHandlerActivator handlerActivator, 
            ILogger logger,
            IHandlerStatusProcessor handlerStatusProcessor,
            IScheduledMessageRepository scheduledMessageRepository,
            int maxConcurrentSessions,
            int defaultWaitSeconds,
            int autoRenewMultiplier)
		{
			_serviceBus = serviceBus;
			_handlerActivator = handlerActivator;
			_logger = logger;
            _handlerStatusProcessor = handlerStatusProcessor;
            _scheduledMessageRepository = scheduledMessageRepository;
            _maxConcurrentSessions = maxConcurrentSessions;
            _defaultWaitSeconds = defaultWaitSeconds;
            _autoRenewMultiplier = autoRenewMultiplier;
        }
        
		/// <summary>
		/// When true, messages are serialized as json which is humanly readable in 
		/// Service Bus Explorer. Otherwise, messages are serialized with Azure
		/// Service Bus' default method which is not humanly readable.
		/// Json serialization is required when using multiple deployments.
		/// Default is true.
		/// </summary>
		public void UseJsonMessageSerialization(bool useJsonSerialization)
		{
			if (!useJsonSerialization && _hasMultipleDeployments)
				throw new ArgumentException("Json serialization is required when using multiple deployments.");

			_useJsonSerialization = useJsonSerialization;
		}

		/// <summary>
		/// When true, tells ServiceBus to only execute EventHandlers once across multiple 
		/// deployments such as a webfarm. Otherwise, event handling a bit simpler in a
		/// standalone deployment where redundancy is not important. Json serialization 
		/// is required when using multiple deployments.
		/// Default is true.
		/// </summary>
		public void HasMultipleDeployments(bool hasMultipleDeployments)
		{
			if (hasMultipleDeployments && !_useJsonSerialization)
				throw new ArgumentException("Json serialization is required when using multiple deployments.");

			_hasMultipleDeployments = hasMultipleDeployments;
		}

        internal async Task CreateQueueAsync(ServiceBusConnection connection, Type type)
        {
            if (_queueClients.ContainsKey(type))
                return;

            var isCommand = typeof(IAggregateCommand).IsAssignableFrom(type);
            
            var client = await _serviceBus.CreateQueueClientAsync(connection, type.FullName, isCommand).ConfigureAwait(false);
            _queueClients.Add(type, client);
        }

        internal async Task CreateHandledQueueAsync(ServiceBusConnection connection, Type handlerType)
        {
            if (_queueHandlers.Values.Contains(handlerType))
                return;

            var interfaces = handlerType.GetInterfaces().Where(i => i.IsGenericType);
            var messageTypes = interfaces.Select(i => i.GetGenericArguments()[0]).Distinct();

            foreach (var messageType in messageTypes)
            {
                await CreateQueueAsync(connection, messageType).ConfigureAwait(false);
                _queueHandlers.Add(messageType, handlerType);
            }
        }

        public async Task SendAsync(ICommand command, DateTime? enqueueTime = null)
        {
            await SendAsync(command, new EnqueueOptions
            {
                EnqueueTime = enqueueTime
            }).ConfigureAwait(false);
        }

		public async Task SendAsync(ICommand command, EnqueueOptions options)
		{
			var isCommand = command is IAggregateCommand;
			var client = _queueClients[command.GetType()];

            // if the enqueue time is 2 seconds or less, the user is really wanting this message
            // to be processed right away. if we proceed and allow the message to be scheduled so 
            // soon, then there is a possibility that azure service bus will reject the request
            if (options.EnqueueTime.HasValue && (options.EnqueueTime.Value - DateTime.UtcNow).TotalSeconds < 2)
                options.EnqueueTime = null;

            var serializer = DataContractBinarySerializer<string>.Instance;
            Message brokeredMessage;
            using (var buffer = new MemoryStream())
            {
                serializer.WriteObject(buffer, JsonConvert.SerializeObject(command));
                brokeredMessage = new Message(buffer.ToArray())
                {
                    MessageId = command.MessageId.ToString(),
                    ContentType = command.GetType().AssemblyQualifiedName
                };
            }
            
			brokeredMessage.ContentType = command.GetType().AssemblyQualifiedName;
			if (isCommand)
			{
				brokeredMessage.SessionId = ((IAggregateCommand) command).GetAggregateId();                
			}
            
		    try
            {
                if (options.EnqueueTime.HasValue)
                {
                    brokeredMessage.ScheduledEnqueueTimeUtc = options.EnqueueTime.Value.ToUniversalTime();
                    var type = command.GetType().FullName;

                    if(options.RemoveAnyExisting && options.RemoveAllButLastInWindowSeconds.HasValue)
                        throw new ArgumentException("RemoveAnyExisting and RemoveAllButLastInWindowSeconds are not supported being set at the same time");
                    
                    if(options.RemoveAnyExisting && options.DuplicatePreventionSeconds.HasValue)
                        throw new ArgumentException("RemoveAnyExisting and DuplicatePreventionSeconds are not supported being set at the same time");

                    if(options.RemoveAllButLastInWindowSeconds.HasValue && options.DuplicatePreventionSeconds.HasValue)
                        throw new ArgumentException("RemoveAllButLastInWindowSeconds and DuplicatePreventionSeconds are not supported being set at the same time");

                    if (options.RemoveAnyExisting)
                    {
                        var preExistingMessages = await _scheduledMessageRepository.GetBySessionIdType(brokeredMessage.SessionId, type).ConfigureAwait(false);
                        
                        // any previously queued message of the same type should be cancelled and removed
                        foreach (var preExistingMessage in preExistingMessages)
                        {
                            await client.CancelScheduledMessageAsync(preExistingMessage.SequenceId).ConfigureAwait(false);
                            await _scheduledMessageRepository.Delete(preExistingMessage.SessionId, preExistingMessage.CorrelationId).ConfigureAwait(false);
                        }
                    }
                    else if (options.RemoveAllButLastInWindowSeconds.HasValue)
                    {
                        var allMessages = (await _scheduledMessageRepository.GetBySessionIdType(brokeredMessage.SessionId, type).ConfigureAwait(false));

                        var messagesInRange = allMessages
                        .Where(x => 
                            x.ScheduleEnqueueDate > DateTime.UtcNow.AddSeconds(-options.RemoveAllButLastInWindowSeconds.Value) && 
                            x.ScheduleEnqueueDate < DateTime.UtcNow.AddSeconds(options.RemoveAllButLastInWindowSeconds.Value) && 
                            !x.IsCancelled
                        )
                        .OrderBy(x => x.ScheduleEnqueueDate)
                        .ToList();
                        
                        var oldMessages = allMessages.Where(x => 
                                x.ScheduleEnqueueDate < DateTime.UtcNow.AddSeconds(-options.RemoveAllButLastInWindowSeconds.Value) && 
                                !x.IsCancelled
                            )
                            .OrderBy(x => x.ScheduleEnqueueDate)
                            .ToList();

                        foreach (var oldMessage in oldMessages)
                        {
                            try
                            {
                                await _scheduledMessageRepository.Delete(oldMessage.SessionId, oldMessage.CorrelationId).ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                _logger.Error(LoggerContext, ex, "Error cleaning up old message {0}", oldMessage.SequenceId);
                            }
                        }

                        // remove all but the last message
                        if (messagesInRange.Count > 0)
                        {
                            foreach (var messageToDelete in messagesInRange.Take(messagesInRange.Count - 1))
                            {
                                try
                                {
                                    await client.CancelScheduledMessageAsync(messageToDelete.SequenceId).ConfigureAwait(false);
                                    await _scheduledMessageRepository.Delete(messageToDelete.SessionId, messageToDelete.CorrelationId).ConfigureAwait(false);
                                }
                                catch (Exception ex)
                                {
                                    try
                                    {
                                        await _scheduledMessageRepository.Cancel(messageToDelete.SessionId, messageToDelete.CorrelationId).ConfigureAwait(false);
                                        _logger.Error(LoggerContext, ex, "Error cancelling message {0}", messageToDelete.SequenceId);
                                    }
                                    catch (Exception ex2)
                                    {
                                        _logger.Error(LoggerContext, ex2, "Error marking message {0} as cancelled", messageToDelete.SequenceId);
                                    }
                                }
                            }

                            // no need to schedule a message, since there is already one queued up for processing in the same timeframe
                            return;
                        }                        
                    }
                    else if (options.DuplicatePreventionSeconds.HasValue)
                    {                        
                        var preExistingMessages = await _scheduledMessageRepository.
                            GetBySessionIdTypeScheduledDateRange(
                                brokeredMessage.SessionId, 
                                type, 
                                options.EnqueueTime.Value.AddSeconds(-options.DuplicatePreventionSeconds.Value), 
                                options.EnqueueTime.Value.AddSeconds(options.DuplicatePreventionSeconds.Value)
                            ).ConfigureAwait(false);

                        // only allow the message to send if there are no already queued messages of the same type
                        if (preExistingMessages.Count > 0)
                        {
                            return;
                        }
                    }
                    

                    brokeredMessage.CorrelationId = Guid.NewGuid().ToString();
                    var sequenceId = await client.ScheduleMessageAsync(brokeredMessage, brokeredMessage.ScheduledEnqueueTimeUtc).ConfigureAwait(false);
                    await _scheduledMessageRepository.Insert(brokeredMessage.SessionId, brokeredMessage.MessageId, sequenceId, type, DateTime.UtcNow, brokeredMessage.ScheduledEnqueueTimeUtc).ConfigureAwait(false);
                }
                else
                {
                    await client.SendAsync(brokeredMessage).ConfigureAwait(false);
                    await client.SendAsync(brokeredMessage).ConfigureAwait(false);
                }
            }
		    catch (Exception ex)
		    {
		        _logger.Error(LoggerContext, "Sending command {0}", ex, command.GetType().ToString());
		    }

		    _logger.Information(LoggerContext, "Sent command {0}", command.GetType().ToString());
		}

        internal async Task CreateTopicAsync(ServiceBusConnection connection, Type type)
        {
            if (_topicClients.ContainsKey(type))
                return;
            
            var client = await _serviceBus.CreateTopicClientAsync(connection, type.FullName).ConfigureAwait(false);
            _topicClients.Add(type, client);
        }

        internal async Task CreateHandledEventAsync(ServiceBusConnection connection, Type handlerType)
        {
            if (_eventHandlers.Values.Any(v => v.Contains(handlerType)))
                return;

            var interfaces = handlerType.GetInterfaces().Where(i => i.IsGenericType);
            var eventTypes = interfaces.Select(i => i.GetGenericArguments()[0]).Distinct();

            foreach (var eventType in eventTypes)
            {
                await CreateTopicAsync(connection, eventType).ConfigureAwait(false);
                if (_eventHandlers.ContainsKey(eventType))
                {
                    _eventHandlers[eventType].Add(handlerType);
                }
                else
                {
                    _eventHandlers.Add(eventType, new HashSet<Type>() { handlerType });
                }
            }
        }

        public async Task PublishAsync(IEvent evt)
        {
            var client = _topicClients[evt.GetType()];

            var isAggregateEvent = evt is IAggregateEvent;

            var serializer = DataContractBinarySerializer<string>.Instance;
            Message brokeredMessage;
            using (var buffer = new MemoryStream())
            {
                serializer.WriteObject(buffer, JsonConvert.SerializeObject(evt));
                brokeredMessage = new Message(buffer.ToArray()) {MessageId = evt.MessageId.ToString()};                
            }

            if (isAggregateEvent)
            {
                var aggregateEvent = ((IAggregateEvent) evt);
                brokeredMessage.SessionId = aggregateEvent.GetAggregateId();
            }

            brokeredMessage.ContentType = evt.GetType().AssemblyQualifiedName;

            _logger.Debug(LoggerContext, "Publishing event {0} to {1}", evt.GetType().Name, client.GetType().Name);

            try
            {
                await client.SendAsync(brokeredMessage).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new Exception($"Error publishing {evt.GetType().Name} {(isAggregateEvent ? ((IAggregateEvent) evt).GetAggregateId() : "")}",ex);
            }
        }        

        public async Task StartHandlers(ServiceBusConnection connection)
        {
            await StartCommandHandlers(connection).ConfigureAwait(false);
            await StartEventHandlers(connection).ConfigureAwait(false);
        }

        private async Task StartEventHandlers(ServiceBusConnection connection)
        {
            foreach (var eventType in _eventHandlers.Keys)
                await StartEventHandler(connection, eventType).ConfigureAwait(false);
        }

        private async Task StartEventHandler(ServiceBusConnection connection, Type eventType)
        {
            try
            {
                var isAggregateEvent = typeof(IAggregateEvent).IsAssignableFrom(eventType);
                var client = await _serviceBus.CreateSubscriptionClientAsync(connection, eventType.FullName, eventType.Name, isAggregateEvent).ConfigureAwait(false);

                if (isAggregateEvent)
                {
                    var options = new SessionHandlerOptions(ExceptionReceivedHandler)
                    {
                        AutoComplete = false,
                        MaxConcurrentSessions = _maxConcurrentSessions
                    };

                    var waitTime = SessionAttribute.GetWaitTimeForType(eventType, _defaultWaitSeconds);
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * _autoRenewMultiplier)
                        : options.MaxAutoRenewDuration;

                    client.RegisterSessionHandler(async (session, message, token) =>
                    {
                        try
                        {
                            await new BusEventHandler(_handlerActivator, _eventHandlers, _logger,
                                    _handlerStatusProcessor, LoggerContext, _defaultWaitSeconds)
                                .OnMessageAsync(async () => await session.RenewSessionLockAsync().ConfigureAwait(false),
                                    session, message).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error(LoggerContext, ex, "Error processing event {0}", eventType.ToString());
                        }
                    }, options);
                }
                else
                {
                    var options = new MessageHandlerOptions(ExceptionReceivedHandler)
                    {
                        AutoComplete = false,
                        MaxConcurrentCalls = _maxConcurrentSessions
                    };
                    var waitTime = SessionAttribute.GetWaitTimeForType(eventType, _defaultWaitSeconds);
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * _autoRenewMultiplier)
                        : options.MaxAutoRenewDuration;

                    client.RegisterMessageHandler(async (message, token) =>
                    {
                        try
                        {
                            await new BusEventHandler(_handlerActivator, _eventHandlers, _logger,
                                    _handlerStatusProcessor, LoggerContext, _defaultWaitSeconds)
                                .OnMessageAsync(async () => { }, client, message).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error(LoggerContext, ex, "Error processing event {0}", eventType.ToString());
                        }
                    }, options);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(LoggerContext, ex, "Error starting event handler for type {0}", eventType.ToString());
            }
        }

        private async Task StartCommandHandlers(ServiceBusConnection connection)
        {
            foreach (var messageType in _queueHandlers.Keys)
                await StartCommandHandler(messageType).ConfigureAwait(false);
        }

        private async Task StartCommandHandler(Type messageType)
        {
            try
            {
                var client = _queueClients[messageType];
                client.PrefetchCount = MessagePrefetchSize;

                if (typeof(IAggregateCommand).IsAssignableFrom(messageType))
                {
                    var options = new SessionHandlerOptions(ExceptionReceivedHandler)
                    {
                        AutoComplete = false,
                        MaxConcurrentSessions = _maxConcurrentSessions
                    };
                    var waitTime = SessionAttribute.GetWaitTimeForType(messageType, _defaultWaitSeconds);
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * _autoRenewMultiplier)
                        : options.MaxAutoRenewDuration;
                    
                    client.RegisterSessionHandler(async (session, message, token) =>
                    {
                        try
                        {
                            await new BusCommandHandler(_handlerActivator, _queueHandlers, _logger,
                                    _handlerStatusProcessor, _scheduledMessageRepository, LoggerContext)
                                .OnMessageAsync(async () => await session.RenewSessionLockAsync().ConfigureAwait(false),
                                    session, message).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error(LoggerContext, ex, "Error processing command {0}", messageType.ToString());
                        }
                    }, options);
                }
                else
                {
                    var options = new MessageHandlerOptions(ExceptionReceivedHandler)
                    {
                        AutoComplete = false,
                        MaxConcurrentCalls = _maxConcurrentSessions,
                    };
                    var waitTime = SessionAttribute.GetWaitTimeForType(messageType, _defaultWaitSeconds);
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * _autoRenewMultiplier)
                        : options.MaxAutoRenewDuration;
                    client.RegisterMessageHandler(async (message, token) =>
                    {
                        try
                        {
                            await new BusCommandHandler(_handlerActivator, _queueHandlers, _logger,
                                    _handlerStatusProcessor, _scheduledMessageRepository, LoggerContext)
                                .OnMessageAsync(async () => { }, client, message).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error(LoggerContext, ex, "Error processing command {0}", messageType.ToString());
                        }
                    }, options);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(LoggerContext, ex, "Error starting command handler for type {0}", messageType.ToString());
            }
        }

        private async Task ExceptionReceivedHandler(ExceptionReceivedEventArgs arg)
        {
            _logger.Error(LoggerContext, arg.Exception, $"Session error received {arg.ExceptionReceivedContext.Action} | { arg.ExceptionReceivedContext.Endpoint} | { arg.ExceptionReceivedContext.ClientId} | { arg.ExceptionReceivedContext.EntityPath}");
        }        
	}
}