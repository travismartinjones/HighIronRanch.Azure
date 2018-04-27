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

	    private static int MessagePrefetchSize = 100;
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

		~ServiceBusWithHandlers()
		{
			Dispose(false);
		}

		public void Dispose()
		{
			Dispose(true);
		}

		protected virtual void Dispose(bool disposing)
		{
            // stop the handlers
            _cancellationTokenSource.Cancel();
            Thread.Sleep(_defaultSessionWaitTime);

            if (!disposing)
			{
				// Unsubscribe
				foreach (var eventType in _eventHandlers.Keys)
				{
					_logger.Information(LoggerContext, "Unsubscribing {0}", eventType.FullName);
					_serviceBus.DeleteSubscriptionAsync(eventType.FullName, eventType.Name);
				}                
			}
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

	        if (type.DoesTypeImplementInterface(typeof(ICommand)))
	        {
	            return await _serviceBus.GetTopicLengthAsync(type.FullName);
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

        internal async Task<MessageSession> AcceptMessageQueueSession(QueueClient client, TimeSpan sessionWaitTime)
		{
		    return await client.AcceptMessageSessionAsync(sessionWaitTime);
		}

	    internal async Task<MessageSession> AcceptMessageSubscriptionSession(SubscriptionClient client, TimeSpan sessionWaitTime)
	    {
	        return await client.AcceptMessageSessionAsync(sessionWaitTime);
	    }

        internal async Task StartQueueSessionAsync(QueueClient client, Func<QueueClient, Task<MessageSession>> clientAccept, Func<BrokeredMessage, Task> messageHandler, OnMessageOptions options, CancellationToken token)
		{            
		    await StartSessionAsync(client, clientAccept, messageHandler, options, token);
        }

	    internal async Task StartSubscriptionSessionAsync(SubscriptionClient client, Func<SubscriptionClient, Task<MessageSession>> clientAccept, Func<BrokeredMessage, Task> messageHandler, OnMessageOptions options, CancellationToken token)
	    {
	        await StartSessionAsync(client, clientAccept, messageHandler, options, token);
	    }

	    internal async Task StartSessionAsync<T>(T client, Func<T, Task<MessageSession>> clientAccept, Func<BrokeredMessage, Task> messageHandler, OnMessageOptions options, CancellationToken token)
	    {
	        while (!token.IsCancellationRequested)
	        {
	            try
	            {
	                var session = await clientAccept(client);	                
                    _logger.Debug(LoggerContext, $"Session accepted: {session.SessionId}");
	                session.OnMessageAsync(messageHandler, options);
	            }
	            catch (TimeoutException)
	            {
	                //_logger.Debug(LoggerContext, ex, "Session timeout: {0}", Thread.CurrentThread.ManagedThreadId);
	                // This is normal. Any logging is noise.
	            }
	            catch (OperationCanceledException ex)
	            {
	                _logger.Information(LoggerContext, ex, "Cancelled: {0}", Thread.CurrentThread.ManagedThreadId);
	            }
	            catch (Exception ex)
	            {
	                _logger.Error(LoggerContext, ex, "Session exception: {0}", ex.Message);                    
	            }
	        }

	        _logger.Debug(LoggerContext, "Cancellation Requested for {0}", messageHandler.Method.Name);
	    }

	    private async Task HandleEventForMultipleDeployments(BrokeredMessage eventToHandle)
	    {
	        try
	        {			    
	            var eventType = Type.GetType(eventToHandle.ContentType);
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
	            _logger.Error("ServiceBusWithHandlers", ex, " Abandoning {0}: {1}", eventToHandle.MessageId, ex.Message);
	            await Task.Delay((int)(100 * Math.Pow(eventToHandle.DeliveryCount, 2)));
	            eventToHandle.Abandon();
	        }
	    }

	    public async Task StartHandlers()
		{
		    var commandSessionHandlerFactory = new CommandSessionHandlerFactory(_handlerActivator, _eventHandlers, _queueHandlers, _logger, LoggerContext, _useJsonSerialization);
            var eventSessionHandlerFactory = new EventSessionHandlerFactory(_handlerActivator, _eventHandlers, _queueHandlers, _logger, LoggerContext, _useJsonSerialization);

#pragma warning disable 4014
		    Task.Run(() =>
		    {
		        Parallel.ForEach(_queueHandlers.Keys, async messageType =>
		        {
		            try
		            {
		                var options = new OnMessageOptions
		                {
		                    AutoComplete = false
		                };
		                var client = _queueClients[messageType];
		                client.PrefetchCount = MessagePrefetchSize;
		                if (typeof(IAggregateCommand).IsAssignableFrom(messageType))
		                {
		                    await client.RegisterSessionHandlerFactoryAsync(
		                        commandSessionHandlerFactory,
		                        new SessionHandlerOptions
		                        {
		                            AutoComplete = false,
		                            AutoRenewTimeout = GetWaitTimeForType(messageType)
		                        });
		                }
		                else
		                {
		                    options.MaxConcurrentCalls = 10;

		                    Task.Run(() =>
		                    {
		                        var commandHandler = new BusCommandHandler(_handlerActivator, _eventHandlers,
		                            _queueHandlers, _logger,
		                            LoggerContext, _useJsonSerialization);
		                        client.OnMessageAsync(async c => await commandHandler.OnMessageAsync(null, c), options);
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
		                var options = new OnMessageOptions
		                {
		                    AutoComplete = false
		                };
		                options.MaxConcurrentCalls = 10;

		                var isAggregateEvent = typeof(IAggregateEvent).IsAssignableFrom(eventType);

		                var client =
		                    await _serviceBus.CreateSubscriptionClientAsync(eventType.FullName, eventType.Name,
		                        isAggregateEvent);

		                if (_hasMultipleDeployments)
		                {
		                    if (isAggregateEvent)
		                    {
		                        await client.RegisterSessionHandlerFactoryAsync(
		                            eventSessionHandlerFactory,
		                            new SessionHandlerOptions
		                            {
		                                AutoComplete = false,
		                                AutoRenewTimeout = GetWaitTimeForType(eventType)
		                            });
		                    }
		                    else
		                        Task.Run(() => client.OnMessageAsync(HandleEventForMultipleDeployments, options));

		                    Task.Run(() =>
		                    {
		                        var qclient = _queueClients[eventType];
		                        var eventHandler = new BusEventHandler(_handlerActivator, _eventHandlers, _queueHandlers,
		                            _logger,
		                            LoggerContext, _useJsonSerialization);
		                        qclient.OnMessageAsync(async e => await eventHandler.OnMessageAsync(null, e), options);
		                    });
		                }
		                else
		                {
		                    if (isAggregateEvent)
		                    {
		                        await client.RegisterSessionHandlerFactoryAsync(
		                            eventSessionHandlerFactory,
		                            new SessionHandlerOptions
		                            {
		                                AutoComplete = false,
		                                AutoRenewTimeout = GetWaitTimeForType(eventType)
		                            });
		                    }
		                    else
		                    {
		                        var eventHandler = new BusEventHandler(_handlerActivator, _eventHandlers, _queueHandlers,
		                            _logger,
		                            LoggerContext, _useJsonSerialization);
		                        Task.Run(() =>
		                            client.OnMessageAsync(async e => await eventHandler.OnMessageAsync(null, e), options));
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