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
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private const int MaximumEventDeliveryCount = 10;

        public BusEventHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, ISet<Type>> eventHandlers,
            ILogger logger,
            string loggerContext,
            bool useJsonSerialization)
        {
            _handlerActivator = handlerActivator;
            _eventHandlers = eventHandlers;
            _logger = logger;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
        }

        public async Task OnMessageAsync(MessageSession session, BrokeredMessage eventToHandle)
        {            
            var eventType = Type.GetType(eventToHandle.ContentType);

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
                    var handler = _handlerActivator.GetInstance(handlerType);
                    var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] {eventType});
                    if (handleMethodInfo == null) continue;
                    var stopwatch = new Stopwatch();
                    _logger.Information(_loggerContext, "Handling Event {0} {1}", eventType, handlerType);
                    stopwatch.Start();
                    await ((Task) handleMethodInfo.Invoke(handler, new[] {message})).ConfigureAwait(false);
                    stopwatch.Stop();
                    _logger.Information(_loggerContext, "Handled Event {0} {1} in {2}s", eventType, handlerType,
                        stopwatch.ElapsedMilliseconds / 1000.0);
                }

                await eventToHandle.CompleteAsync().ConfigureAwait(false);
                if (session != null)
                    await session.CloseAsync().ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                await AbandonMessage(eventToHandle, session).ConfigureAwait(false);
            }
            catch (Exception ex)
            {		        	        
                if (eventToHandle.DeliveryCount < MaximumEventDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(GetDelayFromDeliveryCount(eventToHandle.DeliveryCount)).ConfigureAwait(false);
                    await LogEventError(AlertLevel.Warning, ex, eventToHandle, eventType, session).ConfigureAwait(false);
                    await AbandonMessage(eventToHandle, session).ConfigureAwait(false);
                }
                else
                {
                    await LogEventError(AlertLevel.Error, ex, eventToHandle, eventType, session).ConfigureAwait(false);
                    await AbandonMessage(eventToHandle, session).ConfigureAwait(false);
                }
            }
        }
        
        private int GetDelayFromDeliveryCount(int deliveryCount)
        {            
            switch (deliveryCount)
            {
                case 9:
                    return 5000;
                case 8:
                    return 1000;
                case 7:
                    return 500;
                default:
                    return 100;
            }
        }

        private async Task LogEventError(AlertLevel alertLevel, Exception ex, BrokeredMessage messageToHandle, Type messageType, MessageSession session)
        {
            try
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Event Error {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Event Warning {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Event Info {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                await AbandonMessage(messageToHandle, session);
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

        private static async Task AbandonMessage(BrokeredMessage messageToHandle, MessageSession session)
        {
            await messageToHandle.AbandonAsync().ConfigureAwait(false);
            if (session != null)
                await session.CloseAsync().ConfigureAwait(false);
        }

        public async Task OnCloseSessionAsync(MessageSession session)
        {
            
        }

        public async Task OnSessionLostAsync(Exception exception)
        {
            
        }
    }
}