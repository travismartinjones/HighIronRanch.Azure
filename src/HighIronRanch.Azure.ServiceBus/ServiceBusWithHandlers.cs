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

	    public ServiceBusWithHandlers(IServiceBus serviceBus, IHandlerActivator handlerActivator, ILogger logger)
		{
			_serviceBus = serviceBus;
			_handlerActivator = handlerActivator;
			_logger = logger;
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
			var isCommand = command is IAggregateCommand;
			var client = _queueClients[command.GetType()];            
            
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

		    if (enqueueTime.HasValue)
		    {
		        brokeredMessage.ScheduledEnqueueTimeUtc = enqueueTime.Value.ToUniversalTime();
		    }

		    try
		    {
		        await client.SendAsync(brokeredMessage);
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

			// Create queues for the events in a multiple deployment environment
			if (_hasMultipleDeployments)
			{
				await CreateQueueAsync(type);
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
			await client.SendAsync(brokeredMessage);
		}

	    public async Task<long> GetMessageCount(Type type)
	    {
	        if (type.DoesTypeImplementInterface(typeof(ICommand)))
	        {
	            return await _serviceBus.GetQueueLengthAsync(type.FullName);
	        }

	        if (!type.DoesTypeImplementInterface(typeof(ICommand)))
	        {
	            return await _serviceBus.GetTopicLengthAsync(type.FullName);
	        }

	        return 0;
	    }

	    public async Task<long> GetMessageCount(Type type, string sessionId)
	    {
	        if (type.DoesTypeImplementInterface(typeof(ICommand)))
	        {
	            var isCommand = typeof(IAggregateCommand).IsAssignableFrom(type);
	            return await _serviceBus.GetQueueSessionLengthAsync(type.FullName, isCommand, sessionId);
	        }
            
	        return 0;
	    }

        private TimeSpan GetWaitTimeForType(Type messageType)
        {
            var sessionAttribute = (SessionAttribute)Attribute.GetCustomAttribute(messageType, typeof(SessionAttribute));
            if (sessionAttribute == null)
                return _defaultSessionWaitTime;
            return new TimeSpan(0, 0, sessionAttribute.TimeoutSeconds);
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
	                await Task.Delay((int) (100 * Math.Pow(eventToHandle.DeliveryCount, 2)));
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
		    var commandSessionHandlerFactory = new CommandSessionHandlerFactory(_handlerActivator, _queueHandlers, _logger, LoggerContext, _useJsonSerialization);
            var eventSessionHandlerFactory = new EventSessionHandlerFactory(_handlerActivator, _eventHandlers, _queueHandlers, _logger, LoggerContext, _useJsonSerialization);

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
                            var waitTime = GetWaitTimeForType(messageType);
                            options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                            options.AutoRenewTimeout = options.AutoRenewTimeout < waitTime ? new TimeSpan(waitTime.Ticks * 5) : options.AutoRenewTimeout;

                            await client.RegisterSessionHandlerFactoryAsync(commandSessionHandlerFactory,options);
		                }
		                else
		                {
		                    var commandHandler = new BusCommandHandler(_handlerActivator, _queueHandlers, _logger,
		                        LoggerContext, _useJsonSerialization);
		                    client.OnMessageAsync(async c => await commandHandler.OnMessageAsync(null, c), new OnMessageOptions
		                    {
		                        AutoComplete = false,
		                        MaxConcurrentCalls = 10
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

		                var client = await _serviceBus.CreateSubscriptionClientAsync(eventType.FullName, eventType.Name, isAggregateEvent);

		                if (_hasMultipleDeployments)
		                {
		                    if (isAggregateEvent)
		                    {
		                        var options = new SessionHandlerOptions
		                        {
		                            AutoComplete = false,
		                        };
                                var waitTime = GetWaitTimeForType(eventType);
                                options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                                options.AutoRenewTimeout = options.AutoRenewTimeout < waitTime ? new TimeSpan(waitTime.Ticks * 5) : options.AutoRenewTimeout;
                                await client.RegisterSessionHandlerFactoryAsync(eventSessionHandlerFactory, options);
		                    }
		                    else
		                        client.OnMessageAsync(HandleEventForMultipleDeployments, new OnMessageOptions
		                        {
		                            AutoComplete = false,
		                            MaxConcurrentCalls = 10
		                        });
                            
		                    var qclient = _queueClients[eventType];
		                    var eventHandler = new BusEventHandler(_handlerActivator, _eventHandlers, _logger, LoggerContext, _useJsonSerialization);
		                    qclient.OnMessageAsync(async e => await eventHandler.OnMessageAsync(null, e), new OnMessageOptions
		                    {
		                        AutoComplete = false,
		                        MaxConcurrentCalls = 10
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
                                var waitTime = GetWaitTimeForType(eventType);
                                options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                                options.AutoRenewTimeout = options.AutoRenewTimeout < waitTime ? new TimeSpan(waitTime.Ticks * 5) : options.AutoRenewTimeout;
                                await client.RegisterSessionHandlerFactoryAsync(eventSessionHandlerFactory,options);
		                    }
		                    else
		                    {
		                        var eventHandler = new BusEventHandler(_handlerActivator, _eventHandlers, _logger, LoggerContext, _useJsonSerialization);		                        
		                        client.OnMessageAsync(async e => await eventHandler.OnMessageAsync(null, e), new OnMessageOptions
		                        {
		                            AutoComplete = false,
		                            MaxConcurrentCalls = 10
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