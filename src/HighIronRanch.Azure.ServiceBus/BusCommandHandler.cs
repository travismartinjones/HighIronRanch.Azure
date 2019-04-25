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
        private readonly IScheduledMessageRepository _scheduledMessageRepository;
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private const int MaximumCommandDeliveryCount = 10;

        public BusCommandHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,            
            IScheduledMessageRepository scheduledMessageRepository,
            string loggerContext,
            bool useJsonSerialization)
        {
            _handlerActivator = handlerActivator;
            _queueHandlers = queueHandlers;
            _logger = logger;
            _scheduledMessageRepository = scheduledMessageRepository;
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
                    await ((Task) handleMethodInfo?.Invoke(handler, new[] {message, new CommandActions(messageToHandle)})).ConfigureAwait(false);
                    stopwatch.Stop();
                    _logger.Information(_loggerContext, "Handled Command {0} {1} in {2}s", messageType, handlerType, stopwatch.ElapsedMilliseconds/1000.0);
                }
                
                await _scheduledMessageRepository.Delete(messageToHandle.SessionId, messageToHandle.MessageId);
                await messageToHandle.CompleteAsync().ConfigureAwait(false);
                if(session != null)
                    await session.CloseAsync().ConfigureAwait(false);
            } 
            catch (TimeoutException)
            {
                await AbandonMessage(messageToHandle, session).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (messageToHandle.DeliveryCount < MaximumCommandDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(GetDelayFromDeliveryCount(messageToHandle.DeliveryCount)).ConfigureAwait(false);
                    await LogCommandError(AlertLevel.Warning, ex, messageToHandle, messageType, session).ConfigureAwait(false);
                    await AbandonMessage(messageToHandle, session).ConfigureAwait(false);              
                }
                else
                {
                    await LogCommandError(AlertLevel.Error, ex, messageToHandle, messageType, session).ConfigureAwait(false);
                    await AbandonMessage(messageToHandle, session).ConfigureAwait(false);
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

        private async Task LogCommandError(AlertLevel alertLevel, Exception ex, BrokeredMessage messageToHandle, Type messageType, MessageSession session)
        {
            try
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Command Error {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Command Warning {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Command Info {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                await AbandonMessage(messageToHandle, session).ConfigureAwait(false);
            }
            catch (Exception ex2)
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Command Retry Error {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Command Retry Warning {0} retry {1}", messageType, messageToHandle.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Command Retry Info {0} retry {1}", messageType, messageToHandle.DeliveryCount);
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