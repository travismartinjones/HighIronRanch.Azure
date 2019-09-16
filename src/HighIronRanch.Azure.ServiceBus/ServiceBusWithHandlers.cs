using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

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
        private readonly ISessionService _sessionService;
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
            ISessionService sessionService,
            int maxConcurrentSessions,
            int defaultWaitSeconds,
            int autoRenewMultiplier)
		{
			_serviceBus = serviceBus;
			_handlerActivator = handlerActivator;
			_logger = logger;
            _handlerStatusProcessor = handlerStatusProcessor;
            _scheduledMessageRepository = scheduledMessageRepository;
            _sessionService = sessionService;
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

		internal async Task CreateQueueAsync(Type type)
		{
			if (_queueClients.ContainsKey(type))
				return;

			var isCommand = typeof(IAggregateCommand).IsAssignableFrom(type);

			_logger.Information(LoggerContext, "Creating {0} queue for {1}", isCommand ? "command" : "message", type);

			var client = await _serviceBus.CreateQueueClientAsync(type.FullName, isCommand).ConfigureAwait(false);		    
		    _queueClients.Add(type, client);
		}

		internal async Task CreateHandledQueueAsync(Type handlerType)
		{
			if (_queueHandlers.Values.Contains(handlerType))
				return;

			var interfaces = handlerType.GetInterfaces().Where(i => i.IsGenericType);
			var messageTypes = interfaces.Select(i => i.GetGenericArguments()[0]).Distinct();

			foreach (var messageType in messageTypes)
			{
				await CreateQueueAsync(messageType).ConfigureAwait(false);
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

			BrokeredMessage brokeredMessage;

			if (_useJsonSerialization)
			{
				var message = JsonConvert.SerializeObject(command);
			    brokeredMessage = new BrokeredMessage(message) {MessageId = command.MessageId.ToString()};
			}
			else
			{
				brokeredMessage = new BrokeredMessage(command);
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
                }
            }
		    catch (Exception)
		    {
		        _logger.Error(LoggerContext, "Sending command {0}", command.GetType().ToString());
		    }

		    _logger.Information(LoggerContext, "Sent command {0}", command.GetType().ToString());
		}

		internal async Task CreateTopicAsync(Type type)
		{
			if (_topicClients.ContainsKey(type))
				return;

			_logger.Information(LoggerContext, "Creating topic for {0}", type);

			var client = await _serviceBus.CreateTopicClientAsync(type.FullName).ConfigureAwait(false);            
			_topicClients.Add(type, client);

			// Create queues for the events in a multiple deployment environment
			if (_hasMultipleDeployments)
			{
				await CreateQueueAsync(type).ConfigureAwait(false);
			}
		}

		internal async Task CreateHandledEventAsync(Type handlerType)
		{
			if (_eventHandlers.Values.Any(v => v.Contains(handlerType)))
				return;

			var interfaces = handlerType.GetInterfaces().Where(i => i.IsGenericType);
			var eventTypes = interfaces.Select(i => i.GetGenericArguments()[0]).Distinct();

			foreach (var eventType in eventTypes)
			{
				await CreateTopicAsync(eventType).ConfigureAwait(false);
				if (_eventHandlers.ContainsKey(eventType))
				{
					_eventHandlers[eventType].Add(handlerType);
				}
				else
				{
					_eventHandlers.Add(eventType, new HashSet<Type>() {handlerType});
				}
			}
		}

		public async Task PublishAsync(IEvent evt)
		{
			var client = _topicClients[evt.GetType()];

			BrokeredMessage brokeredMessage;

		    var isAggregateEvent = evt is IAggregateEvent;

            if (_useJsonSerialization)
			{
				var message = JsonConvert.SerializeObject(evt);
			    brokeredMessage = new BrokeredMessage(message) {MessageId = evt.MessageId.ToString()};
			}
			else
			{
				brokeredMessage = new BrokeredMessage(evt);
			}

		    if (isAggregateEvent)
		    {
		        var aggregateEvent = ((IAggregateEvent) evt);
                brokeredMessage.SessionId = aggregateEvent.GetAggregateId();
		    }

			brokeredMessage.ContentType = evt.GetType().AssemblyQualifiedName;

            _logger.Debug(LoggerContext, "Publishing event {0} to {1}", evt.GetType().Name, client.GetType().Name);
			await client.SendAsync(brokeredMessage).ConfigureAwait(false);
		}

	    public async Task<long> GetMessageCount(Type type)
	    {
	        if (type.DoesTypeImplementInterface(typeof(ICommand)))
	        {
	            return await _serviceBus.GetQueueLengthAsync(type.FullName).ConfigureAwait(false);
	        }

	        if (!type.DoesTypeImplementInterface(typeof(ICommand)))
	        {
	            return await _serviceBus.GetTopicLengthAsync(type.FullName).ConfigureAwait(false);
	        }

	        return 0;
	    }

	    public async Task<long> GetMessageCount(Type type, string sessionId)
	    {
	        if (type.DoesTypeImplementInterface(typeof(ICommand)))
	        {
	            var isCommand = typeof(IAggregateCommand).IsAssignableFrom(type);
	            return await _serviceBus.GetQueueSessionLengthAsync(type.FullName, isCommand, sessionId).ConfigureAwait(false);
	        }
            
	        return 0;
	    }
        
	    private async Task HandleEventForMultipleDeployments(BrokeredMessage eventToHandle)
	    {
	        try
	        {			    
	            var eventType = Type.GetType(eventToHandle.ContentType);
	            if (eventType == null) return;
	            // Should be using json serialization
	            var theEvent = JsonConvert.DeserializeObject(eventToHandle.GetBody<string>(), eventType);

	            var client = _queueClients[eventType];

	            var message = JsonConvert.SerializeObject(theEvent);
	            var brokeredMessage = new BrokeredMessage(message)
	            {
	                MessageId = message.GetHashCode().ToString(),
	                ContentType = eventToHandle.ContentType
	            };

	            client.Send(brokeredMessage);
	        }
	        catch (Exception ex)
	        {
	            try
	            {
	                _logger.Error("ServiceBusWithHandlers", ex, " Abandoning {0}: {1}", eventToHandle.MessageId, ex.Message);
	                await Task.Delay((int) (100 * Math.Pow(eventToHandle.DeliveryCount, 2))).ConfigureAwait(false);
	                eventToHandle.Abandon();
	            }
	            catch (Exception ex2)
	            {
	               _logger.Error("ServiceBusWithHandlers", ex, " Abandoning Error {0}: {1}", eventToHandle.MessageId, ex2.Message);
	            }
	        }
	    }

	    public async Task StartHandlers()
		{
		    var commandSessionHandlerFactory = new CommandSessionHandlerFactory(_handlerActivator, _queueHandlers, _logger, _handlerStatusProcessor, _scheduledMessageRepository, _sessionService, LoggerContext, _useJsonSerialization, _defaultWaitSeconds);
            var eventSessionHandlerFactory = new EventSessionHandlerFactory(_handlerActivator, _eventHandlers, _queueHandlers, _logger, _handlerStatusProcessor, _sessionService, LoggerContext, _useJsonSerialization, _defaultWaitSeconds);

#pragma warning disable 4014
		    Task.Run(() =>
		    {
		        Parallel.ForEach(_queueHandlers.Keys, async messageType =>
		        {
		            try
		            {		                
		                var client = _queueClients[messageType];
		                client.PrefetchCount = MessagePrefetchSize;
		                if (typeof(IAggregateCommand).IsAssignableFrom(messageType))
		                {
		                    var options = new SessionHandlerOptions
		                    {
		                        AutoComplete = false,
		                    };
                            var waitTime = SessionAttribute.GetWaitTimeForType(messageType,_defaultWaitSeconds);
                            options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                            options.AutoRenewTimeout = options.AutoRenewTimeout < waitTime ? new TimeSpan(waitTime.Ticks * _autoRenewMultiplier) : options.AutoRenewTimeout;
                            
                            await client.RegisterSessionHandlerFactoryAsync(commandSessionHandlerFactory,options).ConfigureAwait(false);
		                }
		                else
		                {
		                    var commandHandler = new BusCommandHandler(_handlerActivator, _queueHandlers, _logger, _handlerStatusProcessor,  _scheduledMessageRepository,
                                _sessionService, LoggerContext, _useJsonSerialization, _defaultWaitSeconds);
		                    client.OnMessageAsync(async c => await commandHandler.OnMessageAsync(null, c).ConfigureAwait(false), new OnMessageOptions
		                    {
		                        AutoComplete = false,
		                        MaxConcurrentCalls = _maxConcurrentSessions
		                    });
		                }
		            }
		            catch (Exception ex)
		            {
                        _logger.Error(LoggerContext, ex, "Error starting command handler for type {0}",messageType.ToString());
		            }
		        });

		        Parallel.ForEach(_eventHandlers.Keys, async eventType =>
		        {
		            try
		            {
		                var isAggregateEvent = typeof(IAggregateEvent).IsAssignableFrom(eventType);

		                var client = await _serviceBus.CreateSubscriptionClientAsync(eventType.FullName, eventType.Name, isAggregateEvent).ConfigureAwait(false);

		                if (_hasMultipleDeployments)
		                {
		                    if (isAggregateEvent)
		                    {
		                        var options = new SessionHandlerOptions
		                        {
		                            AutoComplete = false,
		                        };
                                var waitTime = SessionAttribute.GetWaitTimeForType(eventType,_defaultWaitSeconds);
                                options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                                options.AutoRenewTimeout = options.AutoRenewTimeout < waitTime ? new TimeSpan(waitTime.Ticks * _autoRenewMultiplier) : options.AutoRenewTimeout;
                                await client.RegisterSessionHandlerFactoryAsync(eventSessionHandlerFactory, options).ConfigureAwait(false);
		                    }
		                    else
		                        client.OnMessageAsync(HandleEventForMultipleDeployments, new OnMessageOptions
		                        {
		                            AutoComplete = false,
		                            MaxConcurrentCalls = _maxConcurrentSessions
		                        });
                            
		                    var qclient = _queueClients[eventType];
		                    var eventHandler = new BusEventHandler(_handlerActivator, _eventHandlers, _logger, _handlerStatusProcessor, _sessionService, LoggerContext, _useJsonSerialization, _defaultWaitSeconds);
		                    qclient.OnMessageAsync(async e => await eventHandler.OnMessageAsync(null, e).ConfigureAwait(false), new OnMessageOptions
		                    {
		                        AutoComplete = false,
		                        MaxConcurrentCalls = _maxConcurrentSessions
		                    });		                    
		                }
		                else
		                {
		                    if (isAggregateEvent)
		                    {
		                        var options = new SessionHandlerOptions
		                        {
		                            AutoComplete = false,
		                        };
                                var waitTime = SessionAttribute.GetWaitTimeForType(eventType, _defaultWaitSeconds);
                                options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                                options.AutoRenewTimeout = options.AutoRenewTimeout < waitTime ? new TimeSpan(waitTime.Ticks * _autoRenewMultiplier) : options.AutoRenewTimeout;
                                await client.RegisterSessionHandlerFactoryAsync(eventSessionHandlerFactory,options).ConfigureAwait(false);
		                    }
		                    else
		                    {
		                        var eventHandler = new BusEventHandler(_handlerActivator, _eventHandlers, _logger, _handlerStatusProcessor, _sessionService, LoggerContext, _useJsonSerialization, _defaultWaitSeconds);		                        
		                        client.OnMessageAsync(async e => await eventHandler.OnMessageAsync(null, e).ConfigureAwait(false), new OnMessageOptions
		                        {
		                            AutoComplete = false,
		                            MaxConcurrentCalls = _maxConcurrentSessions
		                        });
		                    }
		                }
		            }
		            catch (Exception ex)
		            {
		                _logger.Error(LoggerContext, ex, "Error starting event handler for type {0}", eventType.ToString());
		            }
		        });
		    });
		}
	}
}