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
    internal class BusCommandHandler : IMessageSessionAsyncHandler
    {
        private readonly IHandlerActivator _handlerActivator;
        private readonly IDictionary<Type, Type> _queueHandlers;
        private readonly ILogger _logger;
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private const int MaximumCommandDeliveryCount = 10;

        public BusCommandHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,
            string loggerContext,
            bool useJsonSerialization)
        {
            _handlerActivator = handlerActivator;
            _queueHandlers = queueHandlers;
            _logger = logger;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
        }

        public async Task OnMessageAsync(MessageSession session, BrokeredMessage messageToHandle)
        {
            var messageType = Type.GetType(messageToHandle.ContentType);
            try
            {                
                if (messageType == null) return;

                object message;
                if (_useJsonSerialization)
                {
                    message = JsonConvert.DeserializeObject(messageToHandle.GetBody<string>(), messageType);
                }
                else
                {
                    message = messageToHandle
                        ?.GetType()
                        ?.GetMethod("GetBody", new Type[] {})
                        ?.MakeGenericMethod(messageType)
                        .Invoke(messageToHandle, new object[] {});
                }
                
                var handlerType = _queueHandlers[messageType];
                var handler = _handlerActivator.GetInstance(handlerType);

                var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { messageType, typeof(ICommandActions) });
                if (handleMethodInfo != null)
                {
                    var stopwatch = new Stopwatch();					   
                    _logger.Information(_loggerContext, "Handling Command {0} {1}", messageType, handlerType);                        
                    stopwatch.Start();
                    await ((Task) handleMethodInfo?.Invoke(handler, new[] {message, new CommandActions(messageToHandle)}));
                    stopwatch.Stop();
                    _logger.Information(_loggerContext, "Handled Command {0} {1} in {2}s", messageType, handlerType, stopwatch.ElapsedMilliseconds/1000.0);
                }
                
                messageToHandle.Complete();
                if(session != null)
                    await session.CloseAsync();
            }
            catch (Exception ex)
            {
                if (messageToHandle.DeliveryCount < MaximumCommandDeliveryCount)
                {
                    try
                    {
                        // add in exponential spacing between retries
                        await Task.Delay((int) (100 * Math.Pow(messageToHandle.DeliveryCount, 2)));
                        _logger.Error(_loggerContext, ex, "Command Error {0}", messageType);
                        await messageToHandle.AbandonAsync();
                        if (session != null)
                            await session.CloseAsync();
                    }
                    catch (Exception ex2)
                    {
                        _logger.Error(_loggerContext, ex2, "Command Retry Error {0}", messageType);
                    }
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