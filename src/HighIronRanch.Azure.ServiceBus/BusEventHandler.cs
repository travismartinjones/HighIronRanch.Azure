using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class BusEventHandler : IMessageSessionAsyncHandler
    {
        private readonly IHandlerActivator _handlerActivator;
        private readonly IDictionary<Type, ISet<Type>> _eventHandlers;
        private readonly ILogger _logger;
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly ISessionService _sessionService;
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private readonly int _defaultWaitSeconds;
        private const int MaximumEventDeliveryCount = 10;

        public BusEventHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, ISet<Type>> eventHandlers,
            ILogger logger,
            IHandlerStatusProcessor handlerStatusProcessor,
            ISessionService sessionService,
            string loggerContext,
            bool useJsonSerialization,
            int defaultWaitSeconds)
        {
            _handlerActivator = handlerActivator;
            _eventHandlers = eventHandlers;
            _logger = logger;
            _handlerStatusProcessor = handlerStatusProcessor;
            _sessionService = sessionService;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
            _defaultWaitSeconds = defaultWaitSeconds;
        }

        public async Task OnMessageAsync(MessageSession session, BrokeredMessage eventToHandle)
        {            
            var eventType = Type.GetType(eventToHandle.ContentType);
            var stopwatch = new Stopwatch();
            Type lasthandlerType = null;
            try
            {
                _sessionService.Add(session);
                if (eventType == null) return;

                object message;
                if (_useJsonSerialization)
                {
                    message = JsonConvert.DeserializeObject(eventToHandle.GetBody<string>(), eventType);
                }
                else
                {
                    message = eventToHandle
                        ?.GetType()
                        ?.GetMethod("GetBody", new Type[] { })
                        ?.MakeGenericMethod(eventType)
                        .Invoke(eventToHandle, new object[] { });
                }

                var handlerTypes = _eventHandlers[eventType];
              
                var cancellationTokenSource = new CancellationTokenSource();                

                foreach (var handlerType in handlerTypes)
                {
                    lasthandlerType = handlerType;
                    var handler = _handlerActivator.GetInstance(handlerType);

                    var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] {eventType, typeof(IEventActions)});                    
                    if (handleMethodInfo == null) continue;

                    _logger.Information(_loggerContext, "Handling Event {0} {1}", eventType, handlerType);
                    _handlerStatusProcessor.Begin(handlerType.FullName, eventToHandle.SessionId,
                        eventToHandle.EnqueuedTimeUtc);

                    stopwatch.Restart();
                    await ((Task) handleMethodInfo.Invoke(handler, new[] {message, new EventActions(eventToHandle, cancellationTokenSource.Token)})).ConfigureAwait(false);
                    
                    stopwatch.Stop();

                    var elapsedSeconds = stopwatch.ElapsedMilliseconds / 1000.0;
                    _logger.Information(_loggerContext, "Handled Event {0} {1} in {2}s", eventType, handlerType,
                        elapsedSeconds);
                    _handlerStatusProcessor.Complete(handlerType.FullName, eventToHandle.SessionId, elapsedSeconds);
                }

                stopwatch.Restart();
                await eventToHandle.CompleteAsync().ConfigureAwait(false);
                stopwatch.Stop();
                _handlerStatusProcessor.BusComplete(lasthandlerType?.FullName, eventToHandle.SessionId,
                    stopwatch.ElapsedMilliseconds / 1000.0);
            }
            catch (TimeoutException ex)
            {
                _handlerStatusProcessor.Abandon(lasthandlerType?.FullName, eventToHandle.SessionId, ex);
                if (eventToHandle.DeliveryCount < MaximumEventDeliveryCount)
                {
                    await LogAndAbandonEventError(AlertLevel.Warning, ex, eventToHandle, eventType, session)
                        .ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonEventError(AlertLevel.Error, ex, eventToHandle, eventType, session)
                        .ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _handlerStatusProcessor.Error(lasthandlerType?.FullName, eventToHandle.SessionId, ex);
                if (eventToHandle.DeliveryCount < MaximumEventDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(SessionAttribute.GetDelayForType(eventType, eventToHandle.DeliveryCount)).ConfigureAwait(false);
                    await LogAndAbandonEventError(AlertLevel.Warning, ex, eventToHandle, eventType, session)
                        .ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonEventError(AlertLevel.Error, ex, eventToHandle, eventType, session)
                        .ConfigureAwait(false);
                }
            }
            finally
            {
                _sessionService.Remove(session);
            }
        }        

        private async Task LogAndAbandonEventError(AlertLevel alertLevel, Exception ex, BrokeredMessage messageToHandle, Type messageType, MessageSession session)
        {
            try
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Event Error {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Event Warning {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Event Info {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                await AbandonMessage(messageToHandle, session).ConfigureAwait(false);
            }
            catch (Exception ex2)
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Event Retry Error {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Event Retry Warning {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Event Retry Info {0} retry {1}", messageType, messageToHandle.DeliveryCount);
            }
        }

        private async Task AbandonMessage(BrokeredMessage messageToHandle, MessageSession session)
        {            
            var handlerType = Type.GetType(messageToHandle.ContentType);
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            await messageToHandle.AbandonAsync().ConfigureAwait(false);
            stopwatch.Stop();
            _handlerStatusProcessor.BusAbandon(handlerType?.FullName, messageToHandle.SessionId, stopwatch.ElapsedMilliseconds / 1000.0);
        }

        public async Task OnCloseSessionAsync(MessageSession session)
        {
            
        }

        public async Task OnSessionLostAsync(Exception exception)
        {
            
        }
    }
}