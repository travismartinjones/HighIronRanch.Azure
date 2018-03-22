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
    public interface IServiceBusWithHandlers : IDisposable
	{
		void UseJsonMessageSerialization(bool useJsonSerialization);
		Task SendAsync(ICommand command, DateTime? enqueueTime = null);
		Task PublishAsync(IEvent evt);
	    Task<long> GetMessageCount(Type type);
	}

	public class ServiceBusWithHandlers : IServiceBusWithHandlers
	{
		public static readonly string LoggerContext = "HighIronRanch.Azure.ServiceBus";

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
	    private readonly TimeSpan _defaultSessionWaitTime = new TimeSpan(0, 0, 2);

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
				brokeredMessage = new BrokeredMessage(message);
				brokeredMessage.MessageId = command.MessageId.ToString();
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
		    catch (Exception ex)
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
				brokeredMessage = new BrokeredMessage(message);
				brokeredMessage.MessageId = evt.MessageId.ToString();
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
            return new TimeSpan(0,0,sessionAttribute.TimeoutSeconds);
	    }

	    internal async Task StartHandlers()
		{
			foreach (var messageType in _queueHandlers.Keys)
			{
				var options = new OnMessageOptions
				{
				    AutoComplete = false
				};
				var client = _queueClients[messageType];
				if (typeof (IAggregateCommand).IsAssignableFrom(messageType))
				{
#pragma warning disable 4014
                    Task.Run(async () => await StartQueueSessionAsync(client, s => AcceptMessageQueueSession(s, GetWaitTimeForType(messageType)), HandleMessage, options, _cancellationTokenSource.Token));
#pragma warning restore 4014

                }
                else
				{
					options.MaxConcurrentCalls = 10;
#pragma warning disable 4014
                    Task.Run(() => client.OnMessageAsync(HandleMessage, options));
#pragma warning restore 4014

                }
            }

			foreach (var eventType in _eventHandlers.Keys)
			{
				var options = new OnMessageOptions
				{
				    AutoComplete = false
                };
				options.MaxConcurrentCalls = 10;

			    var isAggregateEvent = typeof(IAggregateEvent).IsAssignableFrom(eventType);

                var client = await _serviceBus.CreateSubscriptionClientAsync(eventType.FullName, eventType.Name, isAggregateEvent);

				if (_hasMultipleDeployments)
				{

#pragma warning disable 4014
				    if (isAggregateEvent)
				        Task.Run(async () => await StartSubscriptionSessionAsync(client, s => AcceptMessageSubscriptionSession(s,GetWaitTimeForType(eventType)), HandleEventForMultipleDeployments, options, _cancellationTokenSource.Token));
				    else
				        Task.Run(() => client.OnMessageAsync(HandleEventForMultipleDeployments, options));

				    Task.Run(() =>
                    {
                        var qclient = _queueClients[eventType];
                        qclient.OnMessageAsync(HandleMessage, options);
                    });
#pragma warning restore 4014
                }
                else
				{
#pragma warning disable 4014
				    if (isAggregateEvent)
				        Task.Run(async () =>
				        {
				            await StartSubscriptionSessionAsync(client, s => AcceptMessageSubscriptionSession(s, GetWaitTimeForType(eventType)), HandleEvent, options, _cancellationTokenSource.Token);
				        });
				    else
                        Task.Run(() => client.OnMessageAsync(HandleEvent, options));
#pragma warning restore 4014
                }
            }
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

		private async Task HandleEvent(BrokeredMessage eventToHandle)
		{
		    try
		    {
		        var eventType = Type.GetType(eventToHandle.ContentType);
                
		        object message;
		        if (_useJsonSerialization)
		        {
		            message = JsonConvert.DeserializeObject(eventToHandle.GetBody<string>(), eventType);
		        }
		        else
		        {
		            message = eventToHandle.GetType()
		                .GetMethod("GetBody", new Type[] { })
		                .MakeGenericMethod(eventType)
		                .Invoke(eventToHandle, new object[] { });
		        }

		        var handlerTypes = _eventHandlers[eventType];

		        foreach (var handlerType in handlerTypes)
		        {
		            var handler = _handlerActivator.GetInstance(handlerType);
		            var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] {eventType});
		            if (handleMethodInfo == null) continue;
		            _logger.Information(LoggerContext, "Handling Event {0} {1}", eventType, handlerType);
                    await ((Task) handleMethodInfo.Invoke(handler, new[] {message}));
		        }

		        eventToHandle.Complete();
		    }
		    catch (Exception ex)
		    {		        
		        await Task.Delay((int)(100 * Math.Pow(eventToHandle.DeliveryCount,2)));
		        eventToHandle.Abandon();		        
		    }
		}

		private async Task HandleMessage(BrokeredMessage messageToHandle)
		{
			try
			{
				var messageType = Type.GetType(messageToHandle.ContentType);
                
                object message;
				if (_useJsonSerialization)
				{
					message = JsonConvert.DeserializeObject(messageToHandle.GetBody<string>(), messageType);
				}
				else
				{
					message = messageToHandle.GetType()
								.GetMethod("GetBody", new Type[] {})
								.MakeGenericMethod(messageType)
								.Invoke(messageToHandle, new object[] {});
				}
                
				if (messageType.DoesTypeImplementInterface(typeof(IEvent)))
				{
					var handlerTypes = _eventHandlers[messageType];
					foreach (var handlerType in handlerTypes)
					{
						var handler = _handlerActivator.GetInstance(handlerType);
						var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { messageType });
					    _logger.Information(LoggerContext, "Handling Command {0} {1}", messageType, handlerType);
                        await (Task)handleMethodInfo.Invoke(handler, new[] { message });
					}
				}
				else
				{
					var handlerType = _queueHandlers[messageType];
					var handler = _handlerActivator.GetInstance(handlerType);

					var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { messageType, typeof(ICommandActions) });
					await ((Task)handleMethodInfo?.Invoke(handler, new[] { message, new CommandActions(messageToHandle) }));
				}
				messageToHandle.Complete();
			}
			catch (Exception ex)
			{
                await Task.Delay((int)(100 * Math.Pow(messageToHandle.DeliveryCount, 2)));
                messageToHandle.Abandon();                
			}
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
	            MessageSession session = null;
	            try
	            {
	                session = await clientAccept(client);	                
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
    }
}