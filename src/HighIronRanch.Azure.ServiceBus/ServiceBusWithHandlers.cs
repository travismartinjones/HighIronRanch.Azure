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
		Task SendAsync(IMessage message);
		Task PublishAsync(IEvent evt);
	}

	public class ServiceBusWithHandlers : IServiceBusWithHandlers
	{
		public static readonly string LoggerContext = "HighIronRanch.Azure.ServiceBus";

		private readonly IServiceBus _serviceBus;
		private bool _useJsonSerialization = true;
		private readonly IHandlerActivator _handlerActivator;
		private readonly ILogger _logger;
		protected CancellationToken _cancellationToken = new CancellationToken();

		// IMessage, QueueClient
		protected readonly IDictionary<Type, QueueClient> _queueClients = new Dictionary<Type, QueueClient>();
		
		// IMessage, IMessageHandler/IMessageLongHandler
		protected readonly IDictionary<Type, Type> _queueHandlers = new Dictionary<Type, Type>();

		// IEvent, TopicClient
		protected readonly IDictionary<Type, TopicClient> _topicClients = new Dictionary<Type, TopicClient>();
 
		// IEvent, IEventHandler
		protected readonly IDictionary<Type, Type> _eventHandlers = new Dictionary<Type, Type>();

		// IEventHandler, IEvent
		protected readonly IDictionary<Type, Type> _subscriptions = new Dictionary<Type, Type>(); 

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
				foreach (var eventHandler in _subscriptions.Keys)
				{
					_logger.Information(LoggerContext, "Unsubscribing {0} {1}", _subscriptions[eventHandler].FullName, eventHandler.FullName);
					_serviceBus.DeleteSubscriptionAsync(_subscriptions[eventHandler].FullName, eventHandler.FullName);
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

		public void UseJsonMessageSerialization(bool useJsonSerialization)
		{
			_useJsonSerialization = useJsonSerialization;
		}

		internal async Task CreateQueueAsync(Type type)
		{
			if (_queueClients.ContainsKey(type))
				return;

			var isCommand = typeof(ICommand).IsAssignableFrom(type);

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

		public async Task SendAsync(IMessage message)
		{
			var isCommand = message is ICommand;
			var client = _queueClients[message.GetType()];

			var brokeredMessage = 
				_useJsonSerialization ?
				new BrokeredMessage(JsonConvert.SerializeObject(message)) :
				new BrokeredMessage(message);

			brokeredMessage.ContentType = message.GetType().AssemblyQualifiedName;
			if (isCommand)
			{
				brokeredMessage.SessionId = ((ICommand) message).GetSessionId().ToString();
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
		}

		internal async Task CreateHandledEventAsync(Type handlerType)
		{
			if (_eventHandlers.ContainsKey(handlerType))
				return;

			var interfaces = handlerType.GetInterfaces().Where(i => i.IsGenericType);
			var eventTypes = interfaces.Select(i => i.GetGenericArguments()[0]).Distinct();

			foreach (var eventType in eventTypes)
			{
				await CreateTopicAsync(eventType);
				_eventHandlers.Add(eventType, handlerType);
			}
		}

		public async Task PublishAsync(IEvent evt)
		{
			var client = _topicClients[evt.GetType()];

			var brokeredMessage = 
				_useJsonSerialization ?
				new BrokeredMessage(JsonConvert.SerializeObject(evt)) :
				new BrokeredMessage(evt);

			brokeredMessage.ContentType = evt.GetType().AssemblyQualifiedName;

			await client.SendAsync(brokeredMessage);
		}

		internal async Task StartHandlers()
		{
			foreach (var messageType in _queueHandlers.Keys)
			{
				var options = new OnMessageOptions { };
				var client = _queueClients[messageType];
				if (typeof (ICommand).IsAssignableFrom(messageType))
				{
					Task.Run(() => StartSessionAsync(client, AcceptMessageSession, HandleMessage, options, _cancellationToken));
				}
				else
				{
					options.MaxConcurrentCalls = 10;
					client.OnMessageAsync(HandleMessage, options);
				}
			}

			foreach (var eventType in _eventHandlers.Keys)
			{
				var options = new OnMessageOptions { };
				options.MaxConcurrentCalls = 10;
				var client = await _serviceBus.CreateSubscriptionClientAsync(eventType.FullName, _eventHandlers[eventType].FullName);
				_subscriptions.Add(_eventHandlers[eventType], eventType);
				client.OnMessageAsync(HandleEvent, options);
			}
		}

		private Task HandleEvent(BrokeredMessage eventToHandle)
		{
			try
			{
				var eventType = Type.GetType(eventToHandle.ContentType);

				var message =
					_useJsonSerialization ?
					JsonConvert.DeserializeObject(eventToHandle.GetBody<string>(), eventType) :
					eventToHandle.GetType()
						.GetMethod("GetBody", new Type[] {})
						.MakeGenericMethod(eventType)
						.Invoke(eventToHandle,  new object[] {});

				var handlerType = _eventHandlers[eventType];
				var handler = _handlerActivator.GetInstance(handlerType);

				var handleMethodInfo = handlerType.GetMethod("HandleAsync", new [] {eventType});

				handleMethodInfo.Invoke(handler, new [] {message});

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

				var message =
					_useJsonSerialization
						? JsonConvert.DeserializeObject(messageToHandle.GetBody<string>(), messageType)
						: messageToHandle.GetType()
							.GetMethod("GetBody", new Type[] {})
							.MakeGenericMethod(messageType)
							.Invoke(messageToHandle, new object[] {});

				var handlerType = _queueHandlers[messageType];
				var handler = _handlerActivator.GetInstance(handlerType);

				var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { messageType, typeof(IMessageActions) });
				var task = (Task)handleMethodInfo.Invoke(handler, new[] { message, new MessageActions(messageToHandle) });
				task.Wait();

				messageToHandle.Complete();
			}
			catch (Exception ex)
			{
				Console.WriteLine(" Abandoning {0}: {1}", messageToHandle.MessageId, ex.Message);
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
				catch (TimeoutException)
				{
					_logger.Debug(LoggerContext, string.Format("Session timeout: {0}", Thread.CurrentThread.ManagedThreadId));
				}
				catch (OperationCanceledException)
				{
					_logger.Information(LoggerContext, string.Format("Cancelled: {0}", Thread.CurrentThread.ManagedThreadId));
				}
				catch (Exception ex)
				{
					_logger.Error(LoggerContext, string.Format("Session exception: {0}", ex.Message));
					throw;
				}

				await Task.Run(() => StartSessionAsync(client, clientAccept, messageHandler, options, token), token);
			}
		}
	}
}