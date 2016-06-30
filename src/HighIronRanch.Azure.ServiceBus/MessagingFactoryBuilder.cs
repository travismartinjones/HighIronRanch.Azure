using System;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
#if USE_MESSAGING_FACTORY
    public interface IMessagingFactoryBuilder
    {
        IMessagingFactoryBuilder CreateMessagingFactoryBuilder();
        IMessagingFactoryBuilder WithConnectionString(string connectionString);
        bool IsValid();
        MessagingFactory Build();
    }

    public class MessagingFactoryBuilder : IMessagingFactoryBuilder
    {
        public IMessagingFactoryBuilder CreateMessagingFactoryBuilder()
        {
            return this;
        }

        protected string _connectionString;

        public IMessagingFactoryBuilder WithConnectionString(string connectionString)
        {
            _connectionString = connectionString;
            return this;
        }

        public bool IsValid()
        {
            if (string.IsNullOrEmpty(_connectionString))
                return false;

            return true;
        }

        public MessagingFactory Build()
        {
            if (IsValid())
                return MessagingFactory.CreateFromConnectionString(_connectionString);

            throw new InvalidOperationException("Invalid configuration");
        }
    }
#endif
}