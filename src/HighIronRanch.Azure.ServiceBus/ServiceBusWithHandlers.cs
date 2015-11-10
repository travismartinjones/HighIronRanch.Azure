using System;
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
		Task SendAsync(ICommand command);
		Task PublishAsync(IEvent evt);
	}

	public class ServiceBusWithHandlers : IServiceBusWithHandlers
	{
		public static readonly string LoggerContext = "HighIronRanch.Azure.ServiceBus";

		private readonly IServiceBus _serviceBus;
		private bool _hasMultipleDeployments = true;
		private bool _useJsonSerialization = true;
		private readonly IHandlerActivator _handlerActivator;
		private readonly ILogger _logger;
		protected CancellationToken _cancellationToken = new CancellationToken();

		// ICommand, QueueClient
		protected readonly IDictionary<Type, QueueClient> _queueClients = new Dictionary<Type, QueueClient>();

		// ICommand, ICommandHandler
		protected readonly IDictionary<Type, Type> _queueHandlers = new Dictionary<Type, Type>();

		// IEvent, TopicClient
		protected readonly IDictionary<Type, TopicClient> _topicClients = new Dictionary<Type, TopicClient>();
 
		// IEvent, ISet<IEventHandler>
		protected readonly IDictionary<Type, ISet<Type>> _eventHandlers = new Dictionary<Type, ISet<Type>>(); 

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
			if (!disposing)
			{
				// Unsubscribe
				foreach (var eventType in _eventHandlers.Keys)
				{
					_logger.Information(LoggerContext, "Unsubscribing {0}", eventType.FullName);
					_serviceBus.DeleteSubscriptionAsync(eventType.FullName, eventType.Name);
				}

/*
				foreach (var topicClient in _topicClients.Values)
				{
					topicClient.Close();
				}

				foreach (var queueClient in _queueClients.Values)
				{
					queueClient.Close();
				}
*/
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

		public async Task SendAsync(ICommand command)
		{
			var isCommand = command is IAggregateCommand;
			var client = _queueClients[command.GetType()];

			BrokeredMessage brokeredMessage;

			if (_useJsonSerialization)
			{
				var message = JsonConvert.SerializeObject(command);
				brokeredMessage = new BrokeredMessage(message);
				brokeredMessage.MessageId = message.GetHashCode().ToString();
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

			await client.SendAsync(brokeredMessage);
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

			if (_useJsonSerialization)
			{
				var message = JsonConvert.SerializeObject(evt);
				brokeredMessage = new BrokeredMessage(message);
				brokeredMessage.MessageId = message.GetHashCode().ToString();
			}
			else
			{
				brokeredMessage = new BrokeredMessage(evt);
			}

			brokeredMessage.ContentType = evt.GetType().AssemblyQualifiedName;

            _logger.Debug(LoggerContext, "Publishing event {0} to {1}", evt.GetType().Name, client.GetType().Name);
			await client.SendAsync(brokeredMessage);
		}

		internal async Task StartHandlers()
		{
			foreach (var messageType in _queueHandlers.Keys)
			{
				var options = new OnMessageOptions { };
				var client = _queueClients[messageType];
				if (typeof (IAggregateCommand).IsAssignableFrom(messageType))
				{
#pragma warning disable 4014
					Task.Run(() => StartSessionAsync(client, AcceptMessageSession, HandleMessage, options, _cancellationToken));
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
				var options = new OnMessageOptions { };
				options.MaxConcurrentCalls = 10;

				var client = await _serviceBus.CreateSubscriptionClientAsync(eventType.FullName, eventType.Name);

				if (_hasMultipleDeployments)
				{
					client.OnMessageAsync(HandleEventForMultipleDeployments, options);

					var qclient = _queueClients[eventType];
					qclient.OnMessageAsync(HandleMessage, options);
				}
				else
				{
					client.OnMessageAsync(HandleEvent, options);
				}
			}
		}

		private Task HandleEventForMultipleDeployments(BrokeredMessage eventToHandle)
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
				Console.WriteLine(" Abandoning {0}: {1}", eventToHandle.MessageId, ex.Message);
				eventToHandle.Abandon();
			}

			return Task.FromResult(0);
		}

		private Task HandleEvent(BrokeredMessage eventToHandle)
		{
			try
			{
				var eventType = Type.GetType(eventToHandle.ContentType);

                _logger.Debug(LoggerContext, "Handling event {0}", eventType.Name);

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
					var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { eventType });
					handleMethodInfo.Invoke(handler, new[] { message });
				}

				eventToHandle.Complete();
			}
			catch (Exception ex)
			{
				Console.WriteLine(" Abandoning {0}: {1}", eventToHandle.MessageId, ex.Message);
				eventToHandle.Abandon();
			}

			return Task.FromResult(0);
		}

		private Task HandleMessage(BrokeredMessage messageToHandle)
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
						handleMethodInfo.Invoke(handler, new[] { message });
					}
				}
				else
				{
					var handlerType = _queueHandlers[messageType];
					var handler = _handlerActivator.GetInstance(handlerType);

					var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { messageType, typeof(ICommandActions) });
					var task = (Task)handleMethodInfo.Invoke(handler, new[] { message, new CommandActions(messageToHandle) });
					task.Wait();
				}

				messageToHandle.Complete();
			}
			catch (Exception ex)
			{
                _logger.Warning(LoggerContext, ex, " Abandoning {0}: {1}", messageToHandle.MessageId, ex.Message);
				messageToHandle.Abandon();
			}

			return Task.FromResult(0);
		}

		internal async Task<MessageSession> AcceptMessageSession(QueueClient client)
		{
			return await client.AcceptMessageSessionAsync(new TimeSpan(0, 0, 2));
		}

		internal async Task StartSessionAsync(QueueClient client, Func<QueueClient, Task<MessageSession>> clientAccept, Func<BrokeredMessage, Task> messageHandler, OnMessageOptions options, CancellationToken token)
		{
			if (!token.IsCancellationRequested)
			{
				try
				{
					var session = await clientAccept(client);
					_logger.Debug(LoggerContext, string.Format("Session accepted: {0}", session.SessionId));
					session.OnMessageAsync(messageHandler, options);
				}
				catch (TimeoutException ex)
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

				await Task.Run(() => StartSessionAsync(client, clientAccept, messageHandler, options, token), token);
			}
		}
	}
}