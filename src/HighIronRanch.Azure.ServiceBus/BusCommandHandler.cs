using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Azure.ServiceBus.Standard;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.InteropExtensions;
using Newtonsoft.Json;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class BusCommandHandler
    {
        private readonly IHandlerActivator _handlerActivator;
        private readonly IDictionary<Type, Type> _queueHandlers;
        private readonly ILogger _logger;
        private readonly IScheduledMessageRepository _scheduledMessageRepository;
        private readonly string _loggerContext;
        private const int MaximumCommandDeliveyCount = 10;

        public BusCommandHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,            
            IScheduledMessageRepository scheduledMessageRepository,
            string loggerContext)
        {
            _handlerActivator = handlerActivator;
            _queueHandlers = queueHandlers;
            _logger = logger;
            _scheduledMessageRepository = scheduledMessageRepository;
            _loggerContext = loggerContext;
        }

        public async Task OnMessageAsync(QueueClient queueClient, Message messageToHandle, IMessageSession session)
        {
            var messageType = Type.GetType(messageToHandle.ContentType);
            try
            {                
                if (messageType == null) return;

                // don't process a message if it has been cancelled
                if ((await _scheduledMessageRepository.GetBySessionIdMessageId(messageToHandle.SessionId, messageToHandle.MessageId))?.IsCancelled ?? false)
                    return;

                var message = JsonConvert.DeserializeObject(System.Text.Encoding.UTF8.GetString(messageToHandle.Body), messageType);
                
                var handlerType = _queueHandlers[messageType];
                var handler = _handlerActivator.GetInstance(handlerType);

                var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] { messageType, typeof(ICommandActions) });
                if (handleMethodInfo != null)
                {
                    var stopwatch = new Stopwatch();					   
                    _logger.Information(_loggerContext, "Handling Command {0} {1}", messageType, handlerType);                      
                    stopwatch.Start();
                    await ((Task) handleMethodInfo?.Invoke(handler, new[] {message, new CommandActions(async () =>
                    {
                        if (session == null) return;
                        await session.RenewSessionLockAsync().ConfigureAwait(false);
                    })})).ConfigureAwait(false);
                    stopwatch.Stop();
                    _logger.Information(_loggerContext, "Handled Command {0} {1} in {2}s", messageType, handlerType, stopwatch.ElapsedMilliseconds/1000.0);
                }
                
                await _scheduledMessageRepository.Delete(messageToHandle.SessionId, messageToHandle.MessageId);

                if (session != null)
                {
                    await session.CompleteAsync(messageToHandle.SystemProperties.LockToken).ConfigureAwait(false);
                    await session.CloseAsync();
                }
                else
                    await queueClient.CompleteAsync(messageToHandle.SystemProperties.LockToken).ConfigureAwait(false);
            } 
            catch (TimeoutException)
            {
                await AbandonMessage(messageToHandle, queueClient, session).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (messageToHandle.SystemProperties.DeliveryCount < MaximumCommandDeliveyCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(GetDelayFromDeliveryCount(messageToHandle.SystemProperties.DeliveryCount)).ConfigureAwait(false);
                    await LogCommandError(AlertLevel.Warning, ex, messageToHandle, messageType, queueClient, session).ConfigureAwait(false);
                    await AbandonMessage(messageToHandle, queueClient, session).ConfigureAwait(false);              
                }
                else
                {
                    await LogCommandError(AlertLevel.Error, ex, messageToHandle, messageType, queueClient, session).ConfigureAwait(false);
                    await AbandonMessage(messageToHandle, queueClient, session).ConfigureAwait(false);
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

        private async Task LogCommandError(AlertLevel alertLevel, Exception ex, Message messageToHandle,
            Type messageType, QueueClient client, IMessageSession session)
        {
            try
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Command Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Command Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Command Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                await AbandonMessage(messageToHandle, client, session).ConfigureAwait(false);
            }
            catch
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Command Retry Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Command Retry Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Command Retry Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
            }
        }

        private static async Task AbandonMessage(Message messageToHandle, QueueClient client, IMessageSession session)
        {
            if(session != null)
                await session.AbandonAsync(messageToHandle.SystemProperties.LockToken).ConfigureAwait(false);
            else
                await client.AbandonAsync(messageToHandle.SystemProperties.LockToken).ConfigureAwait(false);
        }        
    }
}