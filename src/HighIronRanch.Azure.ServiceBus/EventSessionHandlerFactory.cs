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
        private readonly string _loggerContext;
        private readonly bool _useJsonSerialization;

        public EventSessionHandlerFactory(
            IHandlerActivator handlerActivator,
            IDictionary<Type, ISet<Type>> eventHandlers,
            IDictionary<Type, Type> queueHandlers,
            ILogger logger,
            string loggerContext,
            bool useJsonSerialization
        )
        {
            _handlerActivator = handlerActivator;
            _eventHandlers = eventHandlers;
            _logger = logger;
            _loggerContext = loggerContext;
            _useJsonSerialization = useJsonSerialization;
        }

        public IMessageSessionAsyncHandler CreateInstance(MessageSession session, BrokeredMessage message)
        {
            return new BusEventHandler(
                _handlerActivator,
                _eventHandlers,
                _logger,
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