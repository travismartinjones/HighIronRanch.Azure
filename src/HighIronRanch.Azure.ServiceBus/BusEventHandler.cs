using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.InteropExtensions;
using Newtonsoft.Json;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class BusEventHandler
    {
        private readonly IHandlerActivator _handlerActivator;
        private readonly IDictionary<Type, ISet<Type>> _eventHandlers;
        private readonly ILogger _logger;
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly string _loggerContext;
        private const int MaximumEventDeliveryCount = 10;

        public BusEventHandler(
            IHandlerActivator handlerActivator,
            IDictionary<Type, ISet<Type>> eventHandlers,
            ILogger logger,
            IHandlerStatusProcessor handlerStatusProcessor,            
            string loggerContext
        )
        {
            _handlerActivator = handlerActivator;
            _eventHandlers = eventHandlers;
            _logger = logger;
            _handlerStatusProcessor = handlerStatusProcessor;            
            _loggerContext = loggerContext;
        }

        public async Task OnMessageAsync(Func<Task> renewAction, IReceiverClient session, Message eventToHandle)
        {            
            var eventType = Type.GetType(eventToHandle.ContentType);
            var stopwatch = new Stopwatch();
            Type lastHandlerType = null;
            try
            {
                if (eventType == null) return;

                var serializer = DataContractBinarySerializer<string>.Instance;
                var bodyContent = (string)serializer.ReadObject(new MemoryStream(eventToHandle.Body));
                var message = JsonConvert.DeserializeObject(bodyContent, eventType);

                var handlerTypes = _eventHandlers[eventType];

                //var cancellationTokenSource = new CancellationTokenSource((int)messageTimeout.TotalMilliseconds);                
                var cancellationTokenSource = new CancellationTokenSource();                

                foreach (var handlerType in handlerTypes)
                {
                    lastHandlerType = handlerType;
                    var handler = _handlerActivator.GetInstance(handlerType);

                    var handleMethodInfo = handlerType.GetMethod("HandleAsync", new[] {eventType, typeof(IEventActions)});                    
                    if (handleMethodInfo == null) continue;

                    _logger.Information(_loggerContext, "Handling Event {0} {1}", eventType, handlerType);
                    _handlerStatusProcessor.Begin(handlerType.FullName, eventToHandle.SessionId,
                        eventToHandle.ScheduledEnqueueTimeUtc);

                    stopwatch.Restart();
                    await ((Task) handleMethodInfo.Invoke(handler, new[] {message, new EventActions(renewAction, eventToHandle, cancellationTokenSource.Token)})).ConfigureAwait(false);
                    
                    stopwatch.Stop();

                    var elapsedSeconds = stopwatch.ElapsedMilliseconds / 1000.0;
                    _logger.Information(_loggerContext, "Handled Event {0} {1} in {2}s", eventType, handlerType,
                        elapsedSeconds);
                    _handlerStatusProcessor.Complete(handlerType.FullName, eventToHandle.SessionId, elapsedSeconds);
                }

                stopwatch.Restart();
                await session.CompleteAsync(eventToHandle.SystemProperties.LockToken).ConfigureAwait(false);                
                stopwatch.Stop();
                _handlerStatusProcessor.BusComplete(lastHandlerType?.FullName, eventToHandle.SessionId,
                    stopwatch.ElapsedMilliseconds / 1000.0);
            }
            catch (TimeoutException ex)
            {
                if (eventToHandle.SystemProperties.DeliveryCount < MaximumEventDeliveryCount)
                {
                    await LogAndAbandonEventError(session, AlertLevel.Warning, ex, eventToHandle, eventType).ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonEventError(session, AlertLevel.Error, ex, eventToHandle, eventType)
                        .ConfigureAwait(false);
                }
                _handlerStatusProcessor.Abandon(lastHandlerType?.FullName ?? eventToHandle.ContentType, eventToHandle.SessionId, ex);
            }
            catch (Exception ex)
            {
                if (eventToHandle.SystemProperties.DeliveryCount < MaximumEventDeliveryCount)
                {
                    // add in exponential spacing between retries
                    await Task.Delay(SessionAttribute.GetDelayForType(eventType, eventToHandle.SystemProperties.DeliveryCount)).ConfigureAwait(false);
                    await LogAndAbandonEventError(session, AlertLevel.Warning, ex, eventToHandle, eventType)
                        .ConfigureAwait(false);
                }
                else
                {
                    await LogAndAbandonEventError(session, AlertLevel.Error, ex, eventToHandle, eventType)
                        .ConfigureAwait(false);
                }
                _handlerStatusProcessor.Error(lastHandlerType?.FullName ?? eventToHandle.ContentType, eventToHandle.SessionId, ex);
            }
        }        

        private async Task LogAndAbandonEventError(IReceiverClient session,  AlertLevel alertLevel, Exception ex, Message messageToHandle, Type messageType)
        {
            try
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex, "Event Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex, "Event Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex, "Event Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                await AbandonMessage(session, messageToHandle).ConfigureAwait(false);
            }
            catch (Exception ex2)
            {
                if(alertLevel == AlertLevel.Error)
                    _logger.Error(_loggerContext, ex2, "Event Retry Error {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Warning)
                    _logger.Warning(_loggerContext, ex2, "Event Retry Warning {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
                else if(alertLevel == AlertLevel.Info)
                    _logger.Information(_loggerContext, ex2, "Event Retry Info {0} retry {1}", messageType, messageToHandle.SystemProperties.DeliveryCount);
            }
        }

        private async Task AbandonMessage(IReceiverClient session, Message messageToHandle)
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