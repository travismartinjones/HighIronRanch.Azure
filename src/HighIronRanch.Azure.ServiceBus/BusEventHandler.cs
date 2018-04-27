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
                    await ((Task) handleMethodInfo.Invoke(handler, new[] {message}));
                    stopwatch.Stop();
                    _logger.Information(_loggerContext, "Handled Event {0} {1} in {2}s", eventType, handlerType, stopwatch.ElapsedMilliseconds/1000.0);
                }                
                eventToHandle.Complete();      
                if(session != null)
                    await session.CloseAsync();
            }
            catch (Exception ex)
            {		        	        
                if (eventToHandle.DeliveryCount < MaximumEventDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay((int)(100 * Math.Pow(eventToHandle.DeliveryCount, 2)));
                    _logger.Error(_loggerContext, ex, "Event Error {0}", eventType);
                    await eventToHandle.AbandonAsync();
                    if(session != null)
                        await session.CloseAsync();
                }
                else
                {
                    throw;
                }
            }
        }

        public async Task OnCloseSessionAsync(MessageSession session)
        {
            
        }

        public async Task OnSessionLostAsync(Exception exception)
        {
            
        }
    }
}