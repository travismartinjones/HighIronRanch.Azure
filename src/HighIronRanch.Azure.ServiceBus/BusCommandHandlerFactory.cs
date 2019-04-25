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
        private readonly IScheduledMessageRepository _scheduledMessageRepository;
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;

        public CommandSessionHandlerFactory(
            IHandlerActivator handlerActivator,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,
            IScheduledMessageRepository scheduledMessageRepository,
            string loggerContext,
            bool useJsonSerialization
        )
        {
            _handlerActivator = handlerActivator;
            _queueHandlers = queueHandlers;
            _logger = logger;
            _scheduledMessageRepository = scheduledMessageRepository;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
        }

        public IMessageSessionAsyncHandler CreateInstance(MessageSession session, BrokeredMessage message)
        {
            return new BusCommandHandler(
                _handlerActivator,
                _queueHandlers,
                _logger,
                _scheduledMessageRepository,
                _loggerContext,
                _useJsonSerialization);
        }

        public void DisposeInstance(IMessageSessionAsyncHandler handler)
        {
            if(handler is IDisposable disposable)
                disposable.Dispose();            
        }
    }
}