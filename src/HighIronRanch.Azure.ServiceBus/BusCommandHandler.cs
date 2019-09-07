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
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private const int MaximumCommandDeliveryCount = 10;

        public BusCommandHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,            
            IHandlerStatusProcessor handlerStatusProcessor,
            IScheduledMessageRepository scheduledMessageRepository,
            string loggerContext,
            bool useJsonSerialization)
        {
            _handlerActivator = handlerActivator;
            _queueHandlers = queueHandlers;
            _logger = logger;
            _scheduledMessageRepository = scheduledMessageRepository;
            _handlerStatusProcessor = handlerStatusProcessor;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
        }

        public async Task OnMessageAsync(MessageSession session, BrokeredMessage messageToHandle)
        {
            var messageType = Type.GetType(messageToHandle.ContentType);            
            var stopwatch = new Stopwatch();					   
            Type handlerType = null;
            try
            {                
                if (messageType == null) return;

                // don't process a message if it has been cancelled
                if ((await _scheduledMessageRepository.GetBySessionIdMessageId(messageToHandle.SessionId, messageToHandle.MessageId))?.IsCancelled ?? false)
                {
                    try
                    {
                        await _scheduledMessageRepository.Delete(messageToHandle.SessionId, messageToHandle.MessageId);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ServiceBusWithHandlers.LoggerContext, ex, "Error removing cancelled message {0}", messageToHandle.MessageId);
                    }

                    return;
                }

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
                
                handlerType = _queueHandlers[messageType];
                var handler = _handlerActivator.GetInstance(handlerType);

                var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { messageType, typeof(ICommandActions) });
                if (handleMethodInfo != null)
                {                    
                    _logger.Information(_loggerContext, "Handling Command {0} {1}", messageType, handlerType);  
                    _handlerStatusProcessor.Begin(handlerType.FullName, messageToHandle.SessionId, messageToHandle.EnqueuedTimeUtc);
                    
                    stopwatch.Restart();
                    await ((Task) handleMethodInfo?.Invoke(handler, new[] {message, new CommandActions(messageToHandle)})).ConfigureAwait(false);
                    stopwatch.Stop();
                    
                    var elapsedSeconds = stopwatch.ElapsedMilliseconds / 1000.0;
                    _logger.Information(_loggerContext, "Handled Command {0} {1} in {2}s", messageType, handlerType, elapsedSeconds);
                    _handlerStatusProcessor.Complete(handlerType.FullName, messageToHandle.SessionId, elapsedSeconds);
                }
                
                await _scheduledMessageRepository.Delete(messageToHandle.SessionId, messageToHandle.MessageId).ConfigureAwait(false);                

                stopwatch.Restart();
                await messageToHandle.CompleteAsync();                
                stopwatch.Stop();
                _handlerStatusProcessor.BusComplete(handlerType?.FullName, messageToHandle.SessionId, stopwatch.ElapsedMilliseconds / 1000.0);                
            } 
            catch (TimeoutException ex)
            {
                _handlerStatusProcessor.Abandon(handlerType?.FullName, messageToHandle.SessionId, ex);
                if (messageToHandle.DeliveryCount < MaximumCommandDeliveryCount)
                {
                    await LogAndAbandonCommandError(AlertLevel.Warning, ex, messageToHandle, messageType, session).ConfigureAwait(false);                
                }
                else
                {
                    await LogAndAbandonCommandError(AlertLevel.Error, ex, messageToHandle, messageType, session).ConfigureAwait(false);                
                }
            }
            catch (Exception ex)
            {
                _handlerStatusProcessor.Error(handlerType?.FullName, messageToHandle.SessionId, ex);
                if (messageToHandle.DeliveryCount < MaximumCommandDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(GetDelayFromDeliveryCount(messageToHandle.DeliveryCount)).ConfigureAwait(false);
                    await LogAndAbandonCommandError(AlertLevel.Warning, ex, messageToHandle, messageType, session).ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonCommandError(AlertLevel.Error, ex, messageToHandle, messageType, session).ConfigureAwait(false);
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

        private async Task LogAndAbandonCommandError(AlertLevel alertLevel, Exception ex, BrokeredMessage messageToHandle, Type messageType, MessageSession session)
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