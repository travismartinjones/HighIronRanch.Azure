using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.InteropExtensions;
using Newtonsoft.Json;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class BusEventHandler
    {
        private readonly IHandlerActivator _handlerActivator;
        private readonly IDictionary<Type, ISet<Type>> _eventHandlers;
        private readonly ILogger _logger;
        private readonly string _loggerContext;
        private const int MaximumEventDeliveryCount = 10;

        public BusEventHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, ISet<Type>> eventHandlers,
            ILogger logger,
            string loggerContext)
        {
            _handlerActivator = handlerActivator;
            _eventHandlers = eventHandlers;
            _logger = logger;
            _loggerContext = loggerContext;
        }

        public async Task OnMessageAsync(SubscriptionClient client, Message eventToHandle)
        {            
            var eventType = Type.GetType(eventToHandle.ContentType);

            try
            {
                if (eventType == null) return;

                var message = JsonConvert.DeserializeObject(eventToHandle.GetBody<string>(), eventType);
                
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

                await client.CompleteAsync(eventToHandle.SystemProperties.LockToken).ConfigureAwait(false);                
            }
            catch (TimeoutException)
            {
                await AbandonMessage(eventToHandle, client).ConfigureAwait(false);
            }
            catch (Exception ex)
            {		        	        
                if (eventToHandle.SystemProperties.DeliveryCount < MaximumEventDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(GetDelayFromDeliveryCount(eventToHandle.SystemProperties.DeliveryCount)).ConfigureAwait(false);
                    await LogEventError(AlertLevel.Warning, ex, eventToHandle, eventType, client).ConfigureAwait(false);
                    await AbandonMessage(eventToHandle, client).ConfigureAwait(false);
                }
                else
                {
                    await LogEventError(AlertLevel.Error, ex, eventToHandle, eventType, client).ConfigureAwait(false);
                    await AbandonMessage(eventToHandle, client).ConfigureAwait(false);
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

        private async Task LogEventError(AlertLevel alertLevel, Exception ex, Message messageToHandle, Type messageType, SubscriptionClient client)
        {
            try
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Event Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Event Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Event Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                await AbandonMessage(messageToHandle, client);
            }
            catch
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Event Retry Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Event Retry Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Event Retry Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
            }
        }

        private static async Task AbandonMessage(Message messageToHandle, SubscriptionClient client)
        {
            await client.AbandonAsync(messageToHandle.SystemProperties.LockToken).ConfigureAwait(false);
        }
    }
}