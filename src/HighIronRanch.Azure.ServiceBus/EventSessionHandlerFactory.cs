using System;
using System.Collections.Generic;
using HighIronRanch.Azure.ServiceBus.Contracts;
using HighIronRanch.Core.Services;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
    internal class EventSessionHandlerFactory : IMessageSessionAsyncHandlerFactory
    {
        private readonly IHandlerActivator _handlerActivator;
        private readonly IDictionary<Type, ISet<Type>> _eventHandlers;
        private readonly ILogger _logger;
        private readonly IHandlerStatusProcessor _handlerStatusProcessor;
        private readonly ISessionService _sessionService;
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;
        private readonly int _defaultWaitSeconds;

        public EventSessionHandlerFactory(IHandlerActivator handlerActivator,
            IDictionary<Type, ISet<Type>> eventHandlers,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,
            IHandlerStatusProcessor handlerStatusProcessor,
            ISessionService sessionService,
            string loggerContext,
            bool useJsonSerialization,
            int defaultWaitSeconds)
        {
            _handlerActivator = handlerActivator;
            _eventHandlers = eventHandlers;
            _logger = logger;
            _handlerStatusProcessor = handlerStatusProcessor;
            _sessionService = sessionService;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
            _defaultWaitSeconds = defaultWaitSeconds;
        }

        public IMessageSessionAsyncHandler CreateInstance(MessageSession session, BrokeredMessage message)
        {
            return new BusEventHandler(
                _handlerActivator,
                _eventHandlers,
                _logger,
                _handlerStatusProcessor,
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