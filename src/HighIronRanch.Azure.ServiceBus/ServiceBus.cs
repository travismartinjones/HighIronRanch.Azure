using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using HighIronRanch.Core.Services;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;

namespace HighIronRanch.Azure.ServiceBus
{
    public interface IServiceBusSettings
    {
        string AzureServiceBusConnectionString { get; }
        string ServiceBusSubscriptionNamePrefix { get; }
        string ServiceBusMasterPrefix { get; }
    }

    public interface IServiceBus
    {
        Task CreateQueueAsync(ServiceBusConnection connection, string name, bool isSessionRequired);
        Task<QueueClient> CreateQueueClientAsync(ServiceBusConnection connection, string name);
        Task<QueueClient> CreateQueueClientAsync(ServiceBusConnection connection, string name, bool isSessionRequired);
        Task<TopicClient> CreateTopicClientAsync(ServiceBusConnection connection, string name);
        Task<SubscriptionClient> CreateSubscriptionClientAsync(ServiceBusConnection connection, string topicName, string subscriptionName, bool isSessionRequired);
    }

    public class ServiceBus : IServiceBus
    {
        private readonly IServiceBusSettings _settings;
        private readonly IServiceBusTypeStateService _serviceBusTypeStateService;
        private readonly ILogger _logger;
        protected ManagementClient _manager;

        public ServiceBus(IServiceBusSettings settings, INamespaceManagerBuilder managerBuilder, IServiceBusTypeStateService serviceBusTypeStateService, ILogger logger)
        {
            _settings = settings;
            _serviceBusTypeStateService = serviceBusTypeStateService;
            _logger = logger;

            _manager = managerBuilder
                .CreateNamespaceBuilder()
                .WithConnectionString(_settings.AzureServiceBusConnectionString)
                .Build();
        }

        protected static string CleanseName(string name)
        {
            // Entity segments can contain only letters, numbers, periods (.), hyphens (-), and underscores (_)
            return Regex.Replace(name, @"[^a-zA-Z0-9\.\-_]", "_").ToLower();
        }

        protected string CreatePrefix()
        {
            if (string.IsNullOrEmpty(_settings.ServiceBusMasterPrefix))
                return "";
            return _settings.ServiceBusMasterPrefix.ToLower() + ".";
        }

        protected string CreateQueueName(string name)
        {
            var queueName = $"q.{CreatePrefix()}{CleanseName(name)}";
            return queueName;
        }

        protected string CreateTopicName(string name)
        {
            var topicName = $"t.{CreatePrefix()}{CleanseName(name)}";
            return topicName;
        }

        protected string CreateSubscriptionName(string name)
        {
            var subname = $"s.{CreatePrefix()}{CleanseName(_settings.ServiceBusSubscriptionNamePrefix)}";

            if (subname.Length > 50)
                throw new ArgumentException($"Resulting subscription {nameof(name)} '{subname}' is longer than 50 character limit", nameof(name));

            return subname;
        }

        public async Task CreateQueueAsync(ServiceBusConnection connection, string name, bool isSessionRequired)
        {
            var queueName = CreateQueueName(name);
            await CreateCleansedNameQueueAsync(connection, queueName, isSessionRequired).ConfigureAwait(false);
        }

        private async Task CreateCleansedNameQueueAsync(ServiceBusConnection connection, string cleansedName, bool isSessionRequired)
        {
            var isPreviouslyCreated = await _serviceBusTypeStateService.GetIsQueueCreated(cleansedName).ConfigureAwait(false);

            if (isPreviouslyCreated) return;

            if (await _manager.QueueExistsAsync(cleansedName).ConfigureAwait(false))
            {
                await _serviceBusTypeStateService.OnQueueCreated(cleansedName).ConfigureAwait(false);
                return;
            }
            
            var qd = new QueueDescription(cleansedName)
            {
                RequiresSession = isSessionRequired,
                EnableDeadLetteringOnMessageExpiration = true,
                RequiresDuplicateDetection = true
            };

            _logger.Information(ServiceBusWithHandlers.LoggerContext, "Creating queue {0}", cleansedName);

            await _manager.CreateQueueAsync(qd).ConfigureAwait(false);
            await _serviceBusTypeStateService.OnQueueCreated(cleansedName).ConfigureAwait(false);
        }

        public async Task<QueueClient> CreateQueueClientAsync(ServiceBusConnection connection, string name)
        {
            return await CreateQueueClientAsync(connection, name, false).ConfigureAwait(false);
        }

        public async Task<QueueClient> CreateQueueClientAsync(ServiceBusConnection connection, string name, bool isSessionRequired)
        {
            var queueName = CreateQueueName(name);            
            await CreateCleansedNameQueueAsync(connection, queueName, isSessionRequired).ConfigureAwait(false);

            return new QueueClient(connection, queueName, ReceiveMode.PeekLock, RetryPolicy.Default);
        }
        
        public async Task<TopicClient> CreateTopicClientAsync(ServiceBusConnection connection, string name)
        {
            var topicName = CreateTopicName(name);
            
            var isPreviouslyCreated = await _serviceBusTypeStateService.GetIsTopicCreated(topicName).ConfigureAwait(false);

            var topicClient = new TopicClient(connection, topicName, RetryPolicy.Default);

            if (isPreviouslyCreated) return topicClient;

            if (await _manager.TopicExistsAsync(topicName).ConfigureAwait(false))
            {
                await _serviceBusTypeStateService.OnTopicCreated(topicName).ConfigureAwait(false);
                return topicClient;
            }

            var td = new TopicDescription(topicName);
            _logger.Information(ServiceBusWithHandlers.LoggerContext, "Creating topic {0}", name);
            await _manager.CreateTopicAsync(td).ConfigureAwait(false);
            await _serviceBusTypeStateService.OnTopicCreated(topicName).ConfigureAwait(false);
            return topicClient;
        }
        
        public async Task<SubscriptionClient> CreateSubscriptionClientAsync(ServiceBusConnection connection, string topicName, string subscriptionName, bool isSessionRequired)
        {
            var cleansedTopicName = CreateTopicName(topicName);
            var cleansedSubscriptionName = CreateSubscriptionName(subscriptionName);

            try
            {
                var subscription = await _manager.GetSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName).ConfigureAwait(false);
                if (subscription.RequiresSession == !isSessionRequired)
                {
                    // the event has changed from/to an aggregate event, so the subscription needs to be removed and re-added to set the session flag
                    await _manager.DeleteSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName).ConfigureAwait(false);
                    subscription.RequiresSession = isSessionRequired;
                    await _manager.CreateSubscriptionAsync(subscription).ConfigureAwait(false);
                }
            }
            catch (MessagingEntityNotFoundException)
            {
                var sd = new SubscriptionDescription(cleansedTopicName, cleansedSubscriptionName);
                sd.RequiresSession = true;
                await _manager.CreateSubscriptionAsync(sd).ConfigureAwait(false);
            }

            return new SubscriptionClient(connection, cleansedTopicName, cleansedSubscriptionName, ReceiveMode.PeekLock, RetryPolicy.Default);
        }        
    }
}