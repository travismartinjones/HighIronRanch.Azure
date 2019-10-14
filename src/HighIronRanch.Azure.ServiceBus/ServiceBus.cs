using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

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
        Task CreateQueueAsync(string name, bool isSessionRequired);
        Task<QueueClient> CreateQueueClientAsync(string name);
        Task<QueueClient> CreateQueueClientAsync(string name, bool isSessionRequired);
        Task DeleteQueueAsync(string name);
        Task<TopicClient> CreateTopicClientAsync(string name);
        Task DeleteTopicAsync(string name);
        Task<SubscriptionClient> CreateSubscriptionClientAsync(string topicName, string subscriptionName, bool isSessionRequired);
        Task DeleteSubscriptionAsync(string topicName, string subscriptionName);
        Task<long> GetQueueLengthAsync(string name);
        Task<long> GetTopicLengthAsync(string name);
        Task<long> GetQueueSessionLengthAsync(string name, bool isCommand, string sessionId);
    }

    public class ServiceBus : IServiceBus
    {
        private readonly IServiceBusSettings _settings;
        private readonly IServiceBusTypeStateService _serviceBusTypeStateService;
        protected NamespaceManager _manager;

        public ServiceBus(IServiceBusSettings settings, INamespaceManagerBuilder managerBuilder, IServiceBusTypeStateService serviceBusTypeStateService)
        {
            _settings = settings;
            _serviceBusTypeStateService = serviceBusTypeStateService;

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
            var queueName = string.Format("q.{0}{1}", CreatePrefix(), CleanseName(name));
            return queueName;
        }

        protected string CreateTopicName(string name)
        {
            var topicName = string.Format("t.{0}{1}", CreatePrefix(), CleanseName(name));
            return topicName;
        }

        protected string CreateSubscriptionName(string name)
        {
            // Use the hashcode to shorten the name and still have a good guarantee of uniqueness
            var subname = string.Format("s.{0}{1}.{2}",
                CreatePrefix(),
                _settings.ServiceBusSubscriptionNamePrefix,
                CleanseName(name).GetHashCode());

            if (subname.Length > 50)
                throw new ArgumentException("Resulting subscription name '" + subname + "' is longer than 50 character limit", "name");

            return subname;
        }

        public async Task CreateQueueAsync(string name, bool isSessionRequired)
        {
            var queueName = CreateQueueName(name);
            await CreateCleansedNameQueueAsync(queueName, isSessionRequired).ConfigureAwait(false);
        }

        private async Task CreateCleansedNameQueueAsync(string cleansedName, bool isSessionRequired)
        {
            var isPreviouslyCreated = await _serviceBusTypeStateService.GetIsQueueCreated(cleansedName).ConfigureAwait(false);

            if (isPreviouslyCreated) return;

            if (_manager.QueueExists(cleansedName))
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
            await _manager.CreateQueueAsync(qd).ConfigureAwait(false);
            await _serviceBusTypeStateService.OnQueueCreated(cleansedName).ConfigureAwait(false);
        }

        public async Task<QueueClient> CreateQueueClientAsync(string name)
        {
            return await CreateQueueClientAsync(name, false).ConfigureAwait(false);
        }

        public async Task<QueueClient> CreateQueueClientAsync(string name, bool isSessionRequired)
        {
            var queueName = CreateQueueName(name);            
            await CreateCleansedNameQueueAsync(queueName, isSessionRequired).ConfigureAwait(false);

            return QueueClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, queueName);
        }

        public async Task DeleteQueueAsync(string name)
        {
            var cleansedName = CreateQueueName(name);
            await _manager.DeleteQueueAsync(cleansedName).ConfigureAwait(false);
        }

        public async Task<TopicClient> CreateTopicClientAsync(string name)
        {
            var topicName = CreateTopicName(name);
            
            var isPreviouslyCreated = await _serviceBusTypeStateService.GetIsTopicCreated(topicName).ConfigureAwait(false);

            var topicClient = TopicClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, topicName);

            if (isPreviouslyCreated) return topicClient;

            if (_manager.TopicExists(topicName))
            {
                await _serviceBusTypeStateService.OnTopicCreated(topicName).ConfigureAwait(false);
                return topicClient;
            }

            var td = new TopicDescription(topicName);
            await _manager.CreateTopicAsync(td).ConfigureAwait(false);
            await _serviceBusTypeStateService.OnTopicCreated(topicName).ConfigureAwait(false);
            return topicClient;
        }

        public async Task DeleteTopicAsync(string name)
        {
            var topicName = CreateTopicName(name);
            await _manager.DeleteTopicAsync(topicName).ConfigureAwait(false);
        }

        public async Task<SubscriptionClient> CreateSubscriptionClientAsync(string topicName, string subscriptionName, bool isSessionRequired)
        {
            var cleansedTopicName = CreateTopicName(topicName);
            var cleansedSubscriptionName = CreateSubscriptionName(subscriptionName);
            var topicSubscriptionName = cleansedTopicName + "|" + cleansedSubscriptionName;

            var isPreviouslyCreated = await _serviceBusTypeStateService.GetIsSubscriptionCreated(topicSubscriptionName).ConfigureAwait(false);

            if (!isPreviouslyCreated)
            {
                try
                {
                    var subscription = await _manager.GetSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName)
                        .ConfigureAwait(false);
                    if (subscription.RequiresSession == !isSessionRequired)
                    {
                        // the event has changed from/to an aggregate event, so the subscription needs to be removed and re-added to set the session flag
                        await _manager.DeleteSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName)
                            .ConfigureAwait(false);
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
            }

            return SubscriptionClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, cleansedTopicName, cleansedSubscriptionName);
        }

        public async Task DeleteSubscriptionAsync(string topicName, string subscriptionName)
        {
            var cleansedTopicName = CreateTopicName(topicName);
            var cleansedSubscriptionName = CreateSubscriptionName(subscriptionName);
            await _manager.DeleteSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName).ConfigureAwait(false);
        }

        public async Task<long> GetQueueLengthAsync(string name)
        {
            var queue = await _manager.GetQueueAsync(CreateQueueName(name)).ConfigureAwait(false);
            return queue.MessageCountDetails.ActiveMessageCount;
        }

        public async Task<long> GetTopicLengthAsync(string name)
        {
            var topic = await _manager.GetTopicAsync(CreateTopicName(name)).ConfigureAwait(false);
            return topic.MessageCountDetails.ActiveMessageCount;
        }

        public async Task<long> GetQueueSessionLengthAsync(string name, bool isCommand, string sessionId)
        {
            var queue = await CreateQueueClientAsync(name, isCommand).ConfigureAwait(false);
            var sessions = await queue.GetMessageSessionsAsync().ConfigureAwait(false);
            return sessions.Count(x => x.SessionId == sessionId);
        }
    }
}