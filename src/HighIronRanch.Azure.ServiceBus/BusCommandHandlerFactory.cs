using System;
using System.Collections.Generic;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class CommandSessionHandlerFactory : IMessageSessionAsyncHandlerFactory
    {
        private readonly IHandlerActivator _handlerActivator;
        private readonly IDictionary<Type, Type> _queueHandlers;
        private readonly ILogger _logger;
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly IScheduledMessageRepository _scheduledMessageRepository;
        private readonly ISessionService _sessionService;
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private readonly int _defaultWaitSeconds;

        public CommandSessionHandlerFactory(
            IHandlerActivator handlerActivator,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,
            IHandlerStatusProcessor handlerStatusProcessor,
            IScheduledMessageRepository scheduledMessageRepository,
            ISessionService sessionService,
            string loggerContext,
            bool useJsonSerialization,
            int defaultWaitSeconds
        )
        {
            _handlerActivator = handlerActivator;
            _queueHandlers = queueHandlers;
            _logger = logger;
            _handlerStatusProcessor = handlerStatusProcessor;
            _scheduledMessageRepository = scheduledMessageRepository;
            _sessionService = sessionService;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
            _defaultWaitSeconds = defaultWaitSeconds;
        }

        public IMessageSessionAsyncHandler CreateInstance(MessageSession session, BrokeredMessage message)
        {
            return new BusCommandHandler(
                _handlerActivator,
                _queueHandlers,
                _logger,
                _handlerStatusProcessor,
                _scheduledMessageRepository,
                _sessionService,
                _loggerContext,
                _useJsonSerialization,
                _defaultWaitSeconds);
        }

        public void DisposeInstance(IMessageSessionAsyncHandler handler)
        {
            if(handler is IDisposable disposable)
                disposable.Dispose();            
        }
    }
}