using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private const int MaximumEventDeliveryCount = 10;

        public BusEventHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, ISet<Type>> eventHandlers,
            ILogger logger,
            IHandlerStatusProcessor handlerStatusProcessor,
            string loggerContext,
            bool useJsonSerialization)
        {
            _handlerActivator = handlerActivator;
            _eventHandlers = eventHandlers;
            _logger = logger;
            _handlerStatusProcessor = handlerStatusProcessor;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
        }

        public async Task OnMessageAsync(MessageSession session, BrokeredMessage eventToHandle)
        {            
            var eventType = Type.GetType(eventToHandle.ContentType);
            var stopwatch = new Stopwatch();
            Type lasthandlerType = null;
            try
            {
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

                foreach (var handlerType in handlerTypes)
                {
                    lasthandlerType = handlerType;
                    var handler = _handlerActivator.GetInstance(handlerType);
                    var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] {eventType});
                    if (handleMethodInfo == null) continue;
                    
                    _logger.Information(_loggerContext, "Handling Event {0} {1}", eventType, handlerType);
                    _handlerStatusProcessor.Begin(handlerType.FullName, eventToHandle.SessionId, eventToHandle.EnqueuedTimeUtc);

                    stopwatch.Restart();             
                    await ((Task) handleMethodInfo.Invoke(handler, new[] {message})).ConfigureAwait(false);
                    stopwatch.Stop();

                    var elapsedSeconds = stopwatch.ElapsedMilliseconds / 1000.0;
                    _logger.Information(_loggerContext, "Handled Event {0} {1} in {2}s", eventType, handlerType, elapsedSeconds);                    
                    _handlerStatusProcessor.Complete(handlerType.FullName, eventToHandle.SessionId, elapsedSeconds);
                }
                
                stopwatch.Restart();
                await eventToHandle.CompleteAsync();                
                stopwatch.Stop();
                _handlerStatusProcessor.BusComplete(lasthandlerType?.FullName, eventToHandle.SessionId, stopwatch.ElapsedMilliseconds / 1000.0);
            }
            catch (TimeoutException ex)
            {
                _handlerStatusProcessor.Abandon(lasthandlerType?.FullName, eventToHandle.SessionId, ex);
                if (eventToHandle.DeliveryCount < MaximumEventDeliveryCount)
                {
                    await LogAndAbandonEventError(AlertLevel.Warning, ex, eventToHandle, eventType, session).ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonEventError(AlertLevel.Error, ex, eventToHandle, eventType, session).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {	
                _handlerStatusProcessor.Error(lasthandlerType?.FullName, eventToHandle.SessionId, ex);
                if (eventToHandle.DeliveryCount < MaximumEventDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(GetDelayFromDeliveryCount(eventToHandle.DeliveryCount)).ConfigureAwait(false);
                    await LogAndAbandonEventError(AlertLevel.Warning, ex, eventToHandle, eventType, session).ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonEventError(AlertLevel.Error, ex, eventToHandle, eventType, session).ConfigureAwait(false);
                }
            }
        }
        
        private int GetDelayFromDeliveryCount(int deliveryCount)
        {            
            switch (deliveryCount)
            {
                case 9:
                    return 1000;
                case 8:
                    return 500;
                case 7:
                    return 100;
                default:
                    return 50;
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