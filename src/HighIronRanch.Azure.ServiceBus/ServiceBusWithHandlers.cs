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

        private int queueSessionCount = 0;
        private int topicSessionCount = 0;

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

        internal async Task CreateQueueAsync(ServiceBusConnection connection, Type type)
        {
            if (_queueClients.ContainsKey(type))
                return;

            var isCommand = typeof(IAggregateCommand).IsAssignableFrom(type);

            _logger.Information(LoggerContext, "Creating {0} queue for {1}", isCommand ? "command" : "message", type);

            var client = await _serviceBus.CreateQueueClientAsync(connection, type.FullName, isCommand);
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
                await CreateQueueAsync(connection, messageType);
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
                MessageId = command.MessageId.ToString(),
                ContentType = command.GetType().AssemblyQualifiedName
            };

            if (isCommand)
            {
                brokeredMessage.SessionId = ((IAggregateCommand)command).GetAggregateId();
            }

            try
            {
                if (options.EnqueueTime.HasValue)
                {
                    brokeredMessage.ScheduledEnqueueTimeUtc = options.EnqueueTime.Value.ToUniversalTime();
                    var type = command.GetType().FullName;

                    if (options.RemoveAnyExisting && options.RemoveAllButLastInWindowSeconds.HasValue)
                        throw new ArgumentException("RemoveAnyExisting and RemoveAllButLastInWindowSeconds are not supported being set at the same time");

                    if (options.RemoveAnyExisting && options.DuplicatePreventionSeconds.HasValue)
                        throw new ArgumentException("RemoveAnyExisting and DuplicatePreventionSeconds are not supported being set at the same time");

                    if (options.RemoveAllButLastInWindowSeconds.HasValue && options.DuplicatePreventionSeconds.HasValue)
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
                            .Where(x => x.ScheduleEnqueueDate > DateTime.UtcNow.AddSeconds(-options.RemoveAllButLastInWindowSeconds.Value) && x.ScheduleEnqueueDate < DateTime.UtcNow.AddSeconds(options.RemoveAllButLastInWindowSeconds.Value))
                            .OrderBy(x => x.ScheduleEnqueueDate)
                            .ToList();
                        
                        // remove all but the last message
                        if (preExistingMessages.Count > 0)
                        {
                            foreach (var messageToDelete in preExistingMessages.Take(preExistingMessages.Count - 1))
                            {
                                try
                                {
                                    await client.CancelScheduledMessageAsync(messageToDelete.SequenceId).ConfigureAwait(false);
                                }
                                catch (Exception ex)
                                {
                                    _logger.Error(LoggerContext, ex, "Error cancelling message {0}", messageToDelete.SequenceId);
                                }
                         
                                try
                                {
                                    await _scheduledMessageRepository.Delete(messageToDelete.SessionId, messageToDelete.CorrelationId);
                                }
                                catch (Exception ex)
                                {
                                    _logger.Error(LoggerContext, ex, "Error deleting message {0}", messageToDelete.SequenceId);
                                }                                
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

        internal async Task CreateTopicAsync(ServiceBusConnection connection, Type type)
        {
            if (_topicClients.ContainsKey(type))
                return;

            _logger.Information(LoggerContext, "Creating topic for {0}", type);

            var client = await _serviceBus.CreateTopicClientAsync(connection, type.FullName);
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
                await CreateTopicAsync(connection, eventType);
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

            var message = JsonConvert.SerializeObject(evt);
            var brokeredMessage = new Message(Encoding.UTF8.GetBytes(message)) { MessageId = evt.MessageId.ToString() };

            if (isAggregateEvent)
            {
                var aggregateEvent = ((IAggregateEvent)evt);
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

        public async Task StartHandlers(ServiceBusConnection connection)
        {
            await StartCommandHandlers(connection);
            await StartEventHandlers(connection);
        }

        private async Task StartEventHandlers(ServiceBusConnection connection)
        {
            foreach (var eventType in _eventHandlers.Keys)
                await StartEventHandler(connection, eventType);
        }

        private async Task StartEventHandler(ServiceBusConnection connection, Type eventType)
        {
            try
            {
                var isAggregateEvent = typeof(IAggregateEvent).IsAssignableFrom(eventType);
                var client = await _serviceBus.CreateSubscriptionClientAsync(connection, eventType.FullName, eventType.Name, isAggregateEvent);

                if (isAggregateEvent)
                {
                    var options = new SessionHandlerOptions(ExceptionReceivedHandler)
                    {
                        AutoComplete = false,
                        MaxConcurrentSessions = 10
                    };
                    var waitTime = GetWaitTimeForType(eventType);
                    options.MessageWaitTimeout = options.MessageWaitTimeout < waitTime
                        ? waitTime
                        : options.MessageWaitTimeout;
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * 5)
                        : options.MaxAutoRenewDuration;
                    _logger.Information(LoggerContext, $"RegisterSessionHandler Topic {++topicSessionCount}");
                    //client.RegisterSessionHandler(async (session, message, cancellationToken) =>
                    //{
                    //    try
                    //    {
                    //        await new BusEventHandler(_handlerActivator, _eventHandlers, _logger, LoggerContext).OnMessageAsync(client, message, session).ConfigureAwait(false);
                    //    }
                    //    catch (Exception ex)
                    //    {
                    //        _logger.Error(LoggerContext, ex, "Error processing event {0}", eventType.ToString());
                    //    }
                    //    finally
                    //    {
                    //        await session.CloseAsync();
                    //    }
                    //}, options);
                }
                else
                {
                    var options = new MessageHandlerOptions(ExceptionReceivedHandler)
                    {
                        AutoComplete = false,
                        MaxConcurrentCalls = 10
                    };
                    var waitTime = GetWaitTimeForType(eventType);
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * 5)
                        : options.MaxAutoRenewDuration;

                    client.RegisterMessageHandler(async (message, cancellationToken) =>
                    {
                        try
                        {
                            await new BusEventHandler(_handlerActivator, _eventHandlers, _logger, LoggerContext).OnMessageAsync(client, message, null).ConfigureAwait(false);
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
                await StartCommandHandler(messageType);
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
                        MaxConcurrentSessions = 10
                    };
                    var waitTime = GetWaitTimeForType(messageType);
                    options.MessageWaitTimeout =
                        options.MessageWaitTimeout < waitTime ? waitTime : options.MessageWaitTimeout;
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * 5)
                        : options.MaxAutoRenewDuration;

                    _logger.Information(LoggerContext, $"RegisterSessionHandler Queue {++queueSessionCount} {messageType}");
                    //client.RegisterSessionHandler(async (session, message, cancellationToken) =>
                    //{
                    //    try
                    //    {
                    //        var commandHandler = new BusCommandHandler(_handlerActivator, _queueHandlers, _logger, _scheduledMessageRepository, LoggerContext);
                    //        await commandHandler.OnMessageAsync(client, message, session);
                    //    }
                    //    catch (Exception ex)
                    //    {
                    //        _logger.Error(LoggerContext, ex, "Error processing command {0}", messageType.ToString());
                    //    }
                    //    finally
                    //    {
                    //        await session.CloseAsync();
                    //    }
                    //}, options);
                }
                else
                {
                    var options = new MessageHandlerOptions(ExceptionReceivedHandler)
                    {
                        AutoComplete = false,
                        MaxConcurrentCalls = 10
                    };
                    var waitTime = GetWaitTimeForType(messageType);
                    options.MaxAutoRenewDuration = options.MaxAutoRenewDuration < waitTime
                        ? new TimeSpan(waitTime.Ticks * 5)
                        : options.MaxAutoRenewDuration;

                    var commandHandler = new BusCommandHandler(_handlerActivator, _queueHandlers, _logger, _scheduledMessageRepository, LoggerContext);
                    client.RegisterMessageHandler(async (message, cancellationToken) =>
                    {
                        try
                        {
                            await commandHandler.OnMessageAsync(client, message, null);
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