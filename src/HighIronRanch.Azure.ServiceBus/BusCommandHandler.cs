using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Azure.ServiceBus.Standard;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
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
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly string _loggerContext;
        private const int MaximumCommandDeliveryCount = 10;

        public BusCommandHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,            
            IHandlerStatusProcessor handlerStatusProcessor,
            IScheduledMessageRepository scheduledMessageRepository,
            string loggerContext)
        {
            _handlerActivator = handlerActivator;
            _queueHandlers = queueHandlers;
            _logger = logger;
            _scheduledMessageRepository = scheduledMessageRepository;
            _handlerStatusProcessor = handlerStatusProcessor;
            _loggerContext = loggerContext;
        }

        public async Task OnMessageAsync(Func<Task> renewAction, IReceiverClient session, Message messageToHandle)
        {            
            var messageType = Type.GetType(messageToHandle.ContentType);
            var stopwatch = new Stopwatch();					   
            Type handlerType = null;
            try
            {
                if (messageType == null) return;

                // don't process a message if it has been cancelled
                if ((await _scheduledMessageRepository.GetBySessionIdMessageId(messageToHandle.SessionId,
                        messageToHandle.MessageId).ConfigureAwait(false))?.IsCancelled ?? false)
                {
                    try
                    {
                        await _scheduledMessageRepository.Delete(messageToHandle.SessionId, messageToHandle.MessageId).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(ServiceBusWithHandlers.LoggerContext, ex, "Error removing cancelled message {0}",
                            messageToHandle.MessageId);
                    }

                    return;
                }

                var serializer = DataContractBinarySerializer<string>.Instance;
                var bodyContent = (string)serializer.ReadObject(new MemoryStream(messageToHandle.Body));
                
                var message = JsonConvert.DeserializeObject(bodyContent, messageType);             
                handlerType = _queueHandlers[messageType];
                var handler = _handlerActivator.GetInstance(handlerType);

                var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] {messageType, typeof(ICommandActions)});
                
                if (handleMethodInfo != null)
                {
                    _logger.Information(_loggerContext, "Handling Command {0} {1}", messageType, handlerType);
                    _handlerStatusProcessor.Begin(handlerType.FullName, messageToHandle.SessionId,
                        messageToHandle.ScheduledEnqueueTimeUtc);

                    //var cancellationTokenSource = new CancellationTokenSource((int)messageTimeout.TotalMilliseconds);
                    var cancellationTokenSource = new CancellationTokenSource();

                    stopwatch.Restart();                    
                    await ((Task) handleMethodInfo?.Invoke(handler, new[] {message, new CommandActions(renewAction, messageToHandle, cancellationTokenSource.Token)})).ConfigureAwait(false);
                    stopwatch.Stop();

                    var elapsedSeconds = stopwatch.ElapsedMilliseconds / 1000.0;
                    _logger.Information(_loggerContext, "Handled Command {0} {1} in {2}s", messageType, handlerType,
                        elapsedSeconds);
                    _handlerStatusProcessor.Complete(handlerType.FullName, messageToHandle.SessionId, elapsedSeconds);
                }

                await _scheduledMessageRepository.Delete(messageToHandle.SessionId, messageToHandle.MessageId)
                    .ConfigureAwait(false);

                stopwatch.Restart();
                await session.CompleteAsync(messageToHandle.SystemProperties.LockToken).ConfigureAwait(false);
                stopwatch.Stop();
                _handlerStatusProcessor.BusComplete(handlerType?.FullName, messageToHandle.SessionId,
                    stopwatch.ElapsedMilliseconds / 1000.0);
            }
            catch (TimeoutException ex)
            {
                if (messageToHandle.SystemProperties.DeliveryCount < MaximumCommandDeliveryCount)
                {
                    await LogAndAbandonCommandError(AlertLevel.Warning, ex, messageToHandle, messageType, session)
                        .ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonCommandError(AlertLevel.Error, ex, messageToHandle, messageType, session)
                        .ConfigureAwait(false);
                }
                _handlerStatusProcessor.Abandon(handlerType?.FullName ?? messageToHandle.ContentType, messageToHandle.SessionId, ex);
            }
            catch (Exception ex)
            {
                if (messageToHandle.SystemProperties.DeliveryCount < MaximumCommandDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(SessionAttribute.GetDelayForType(messageType, messageToHandle.SystemProperties.DeliveryCount)).ConfigureAwait(false);
                    await LogAndAbandonCommandError(AlertLevel.Warning, ex, messageToHandle, messageType, session)
                        .ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonCommandError(AlertLevel.Error, ex, messageToHandle, messageType, session)
                        .ConfigureAwait(false);
                }
                _handlerStatusProcessor.Error(handlerType?.FullName ?? messageToHandle.ContentType, messageToHandle.SessionId, ex);
            }
        }

        
        private async Task LogAndAbandonCommandError(AlertLevel alertLevel, Exception ex, Message messageToHandle, Type messageType, IReceiverClient session)
        {
            try
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Command Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Command Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Command Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                await AbandonMessage(messageToHandle, session).ConfigureAwait(false);
            }
            catch (Exception ex2)
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Command Retry Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Command Retry Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Command Retry Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
            }
        }

        private async Task AbandonMessage(Message messageToHandle, IReceiverClient session)
        {
            var handlerType = Type.GetType(messageToHandle.ContentType);
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            await session.AbandonAsync(messageToHandle.SystemProperties.LockToken).ConfigureAwait(false);
            stopwatch.Stop();
            _handlerStatusProcessor.BusAbandon(handlerType?.FullName, messageToHandle.SessionId, stopwatch.ElapsedMilliseconds / 1000.0);            
        }
    }
}