using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.InteropExtensions;
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
		private readonly IHandlerActivator _handlerActivator;
		private readonly ILogger _logger;
        private readonly IScheduledMessageRepository _scheduledMessageRepository;
        protected CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        
		// ICommand, QueueClient
		protected readonly IDictionary<Type, QueueClient> _queueClients = new ConcurrentDictionary<Type, QueueClient>();

		// ICommand, ICommandHandler
		protected readonly IDictionary<Type, Type> _queueHandlers = new ConcurrentDictionary<Type, Type>();

		// IEvent, TopicClient
		protected readonly IDictionary<Type, TopicClient> _topicClients = new ConcurrentDictionary<Type, TopicClient>();
 
		// IEvent, ISet<IEventHandler>
		protected readonly IDictionary<Type, ISet<Type>> _eventHandlers = new ConcurrentDictionary<Type, ISet<Type>>();
	    private readonly TimeSpan _defaultSessionWaitTime = new TimeSpan(0, 0, 60);

	    public ServiceBusWithHandlers(
            IServiceBus serviceBus, 
            IHandlerActivator handlerActivator, 
            ILogger logger,
            IScheduledMessageRepository scheduledMessageRepository)
		{
			_serviceBus = serviceBus;
			_handlerActivator = handlerActivator;
			_logger = logger;
            _scheduledMessageRepository = scheduledMessageRepository;
        }        
        
		internal async Task CreateQueueAsync(Type type)
		{
			if (_queueClients.ContainsKey(type))
				return;

			var isCommand = typeof(IAggregateCommand).IsAssignableFrom(type);

			_logger.Information(LoggerContext, "Creating {0} queue for {1}", isCommand ? "command" : "message", type);

			var client = await _serviceBus.CreateQueueClientAsync(type.FullName, isCommand);		    
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
				await CreateQueueAsync(messageType);
				_queueHandlers.Add(messageType, handlerType);
			}
		}

        public async Task SendAsync(ICommand command, DateTime? enqueueTime = null)
        {
            await SendAsync(command, new EnqueueOptions
            {
                EnqueueTime = enqueueTime
            });
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

            var message = JsonConvert.SerializeObject(command);
            var brokeredMessage = new Message(Encoding.UTF8.GetBytes(message))
            {
                MessageId = command.MessageId.ToString(), ContentType = command.GetType().Name
            };

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
                        var preExistingMessages = await _scheduledMessageRepository.GetBySessionIdType(brokeredMessage.SessionId, type);
                        
                        // any previously queued message of the same type should be cancelled and removed
                        foreach (var preExistingMessage in preExistingMessages)
                        {
                            await client.CancelScheduledMessageAsync(preExistingMessage.SequenceId);
                            await _scheduledMessageRepository.Delete(preExistingMessage.SessionId, preExistingMessage.CorrelationId);
                        }
                    }
                    else if (options.RemoveAllButLastInWindowSeconds.HasValue)
                    {
                        var preExistingMessages = (await _scheduledMessageRepository.GetBySessionIdType(brokeredMessage.SessionId, type))
                            .Where(x => x.ScheduleEnqueueDate > DateTime.UtcNow && x.ScheduleEnqueueDate < DateTime.UtcNow.AddSeconds(options.RemoveAllButLastInWindowSeconds.Value))
                            .ToList();
                        
                        // remove all but the last message
                        if (preExistingMessages.Count > 0)
                        {
                            foreach (var messageToDelete in preExistingMessages.Take(preExistingMessages.Count - 1))
                            {
                                await client.CancelScheduledMessageAsync(messageToDelete.SequenceId);
                                await _scheduledMessageRepository.Delete(messageToDelete.SessionId, messageToDelete.CorrelationId);
                            }
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
                            );

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

			var client = await _serviceBus.CreateTopicClientAsync(type.FullName);            
			_topicClients.Add(type, client);
            
			await CreateQueueAsync(type);
		}

		internal async Task CreateHandledEventAsync(Type handlerType)
		{
			if (_eventHandlers.Values.Any(v => v.Contains(handlerType)))
				return;

			var interfaces = handlerType.GetInterfaces().Where(i => i.IsGenericType);
			var eventTypes = interfaces.Select(i => i.GetGenericArguments()[0]).Distinct();

			foreach (var eventType in eventTypes)
			{
				await CreateTopicAsync(eventType);
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

            var isAggregateEvent = evt is IAggregateEvent;
            
			var message = JsonConvert.SerializeObject(evt);
			var brokeredMessage = new Message(Encoding.UTF8.GetBytes(message)) {MessageId = evt.MessageId.ToString()};
			
		    if (isAggregateEvent)
		    {
		        var aggregateEvent = ((IAggregateEvent) evt);
                brokeredMessage.SessionId = aggregateEvent.GetAggregateId();
		    }

			brokeredMessage.ContentType = evt.GetType().AssemblyQualifiedName;

            _logger.Debug(LoggerContext, "Publishing event {0} to {1}", evt.GetType().Name, client.GetType().Name);
			await client.SendAsync(brokeredMessage);
		}
        
        private TimeSpan GetWaitTimeForType(Type messageType)
        {
            var sessionAttribute = (SessionAttribute)Attribute.GetCustomAttribute(messageType, typeof(SessionAttribute));
            if (sessionAttribute == null)
                return _defaultSessionWaitTime;
            return new TimeSpan(0, 0, sessionAttribute.TimeoutSeconds);
        }
        
	    private async Task HandleEventForMultipleDeployments(Message eventToHandle)
	    {
            var eventType = Type.GetType(eventToHandle.ContentType);
            if (eventType == null) return;
            var client = _queueClients[eventType];

	        try
	        {			    	            	            
	            // Should be using json serialization
	            var theEvent = JsonConvert.DeserializeObject(eventToHandle.GetBody<string>(), eventType);	            

	            var message = JsonConvert.SerializeObject(theEvent);
	            var brokeredMessage = new Message(Encoding.UTF8.GetBytes(message))
	            {
	                MessageId = message.GetHashCode().ToString(),
	                ContentType = eventToHandle.ContentType
	            };

	            await client.SendAsync(brokeredMessage);
	        }
	        catch (Exception ex)
	        {
	            try
	            {
	                _logger.Error("ServiceBusWithHandlers", ex, " Abandoning {0}: {1}", eventToHandle.MessageId, ex.Message);
	                await Task.Delay((int) (100 * Math.Pow(eventToHandle.SystemProperties.DeliveryCount, 2)));
	                await client.AbandonAsync(eventToHandle.SystemProperties.LockToken);
	            }
	            catch (Exception ex2)
	            {
	               _logger.Error("ServiceBusWithHandlers", ex, " Abandoning Error {0}: {1}", eventToHandle.MessageId, ex2.Message);
	            }
	        }
	    }

	    public async Task StartHandlers()
		{		    
            StartCommandHandlers();
            StartEventHandlers();
		}

        private void StartEventHandlers()
        {
            Parallel.ForEach(_eventHandlers.Keys, async eventType =>
            {
                try
                {
                    var isAggregateEvent = typeof(IAggregateEvent).IsAssignableFrom(eventType);
                    var client = await _serviceBus.CreateSubscriptionClientAsync(eventType.FullName, eventType.Name, isAggregateEvent);

                    if (isAggregateEvent)
                    {
                        var options = new SessionHandlerOptions(ExceptionReceivedHandler)
                        {
                            AutoComplete = false,
                        };
                        var waitTime = GetWaitTimeForType(eventType);
                        options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime
                            ? waitTime
                            : options.MessageWaitTimeout;
                        options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                            ? new TimeSpan(waitTime.Ticks * 5)
                            : options.MaxAutoRenewDuration;
                        
                        client.RegisterSessionHandler(async (session, message, cancellationToken) =>
                        {
                            await new BusEventHandler(_handlerActivator, _eventHandlers, _logger, LoggerContext).OnMessageAsync(client, message).ConfigureAwait(false);
                        }, options);
                    }
                    else
                    {                     
                        var options = new MessageHandlerOptions(ExceptionReceivedHandler)
                        {
                            AutoComplete = false,
                        };
                        var waitTime = GetWaitTimeForType(eventType);
                        options.MaxConcurrentCalls = 10;
                        options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                            ? new TimeSpan(waitTime.Ticks * 5)
                            : options.MaxAutoRenewDuration;

                        client.RegisterMessageHandler(async (message, cancellationToken) =>
                        {
                            await new BusEventHandler(_handlerActivator, _eventHandlers, _logger, LoggerContext).OnMessageAsync(client, message).ConfigureAwait(false);
                        }, options);
                    }                    
                }
                catch (Exception ex)
                {
                    _logger.Error(LoggerContext, ex, "Error starting event handler for type {0}", eventType.ToString());
                }
            });
        }

        private void StartCommandHandlers()
        {
            Parallel.ForEach(_queueHandlers.Keys, async messageType =>
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
                        };
                        var waitTime = GetWaitTimeForType(messageType);
                        options.MessageWaitTimeout =
                            options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                        options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                            ? new TimeSpan(waitTime.Ticks * 5)
                            : options.MaxAutoRenewDuration;

                        client.RegisterSessionHandler(async (session, message, cancellationToken) =>
                        {
                            var commandHandler = new BusCommandHandler(_handlerActivator, _queueHandlers, _logger, _scheduledMessageRepository, LoggerContext);
                            await commandHandler.OnMessageAsync(client, message, async () => await session.RenewSessionLockAsync().ConfigureAwait(false));
                        }, options);
                    }
                    else
                    {
                        var options = new MessageHandlerOptions(ExceptionReceivedHandler)
                        {
                            AutoComplete = false,
                        };
                        var waitTime = GetWaitTimeForType(messageType);
                        options.MaxConcurrentCalls = 10;
                        options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                            ? new TimeSpan(waitTime.Ticks * 5)
                            : options.MaxAutoRenewDuration;

                        var commandHandler = new BusCommandHandler(_handlerActivator, _queueHandlers, _logger, _scheduledMessageRepository, LoggerContext);
                        client.RegisterMessageHandler(async (message, cancellationToken) =>
                        {
                            await commandHandler.OnMessageAsync(client, message, async () => { });
                        }, options);
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error(LoggerContext, ex, "Error starting command handler for type {0}", messageType.ToString());
                }
            });
        }

        private async Task ExceptionReceivedHandler(ExceptionReceivedEventArgs arg)
        {
            _logger.Error(LoggerContext, arg.Exception, $"Session error received {arg.ExceptionReceivedContext.Action} | { arg.ExceptionReceivedContext.Endpoint} | { arg.ExceptionReceivedContext.ClientId} | { arg.ExceptionReceivedContext.EntityPath}");
        }
    }
}