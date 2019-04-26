using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
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
        Task CreateQueueAsync(string name, bool isSessionRequired);
        Task<QueueClient> CreateQueueClientAsync(string name);
        Task<QueueClient> CreateQueueClientAsync(string name, bool isSessionRequired);
        Task<TopicClient> CreateTopicClientAsync(string name);
        Task<SubscriptionClient> CreateSubscriptionClientAsync(string topicName, string subscriptionName, bool isSessionRequired);
    }

    public class ServiceBus : IServiceBus
    {
        private readonly IServiceBusSettings _settings;
        private readonly IServiceBusTypeStateService _serviceBusTypeStateService;
        protected ManagementClient _manager;

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
            await CreateCleansedNameQueueAsync(queueName, isSessionRequired);
        }

        private async Task CreateCleansedNameQueueAsync(string cleansedName, bool isSessionRequired)
        {
            var isPreviouslyCreated = await _serviceBusTypeStateService.GetIsQueueCreated(cleansedName);

            if (isPreviouslyCreated) return;

            if (await _manager.QueueExistsAsync(cleansedName))
            {
                await _serviceBusTypeStateService.OnQueueCreated(cleansedName);
                return;
            }
            
            var qd = new QueueDescription(cleansedName)
            {
                RequiresSession = isSessionRequired,
                EnableDeadLetteringOnMessageExpiration = true,
                RequiresDuplicateDetection = true
            };
            await _manager.CreateQueueAsync(qd);
            await _serviceBusTypeStateService.OnQueueCreated(cleansedName);
        }

        public async Task<QueueClient> CreateQueueClientAsync(string name)
        {
            return await CreateQueueClientAsync(name, false);
        }

        public async Task<QueueClient> CreateQueueClientAsync(string name, bool isSessionRequired)
        {
            var queueName = CreateQueueName(name);            
            await CreateCleansedNameQueueAsync(queueName, isSessionRequired);

            return new QueueClient(_settings.AzureServiceBusConnectionString, queueName);
        }
        
        public async Task<TopicClient> CreateTopicClientAsync(string name)
        {
            var topicName = CreateTopicName(name);
            
            var isPreviouslyCreated = await _serviceBusTypeStateService.GetIsTopicCreated(topicName);

            var topicClient = new TopicClient(_settings.AzureServiceBusConnectionString, topicName);;

            if (isPreviouslyCreated) return topicClient;

            if (await _manager.TopicExistsAsync(topicName))
            {
                await _serviceBusTypeStateService.OnTopicCreated(topicName);
                return topicClient;
            }

            var td = new TopicDescription(topicName);
            await _manager.CreateTopicAsync(td);
            await _serviceBusTypeStateService.OnTopicCreated(topicName);
            return topicClient;
        }
        
        public async Task<SubscriptionClient> CreateSubscriptionClientAsync(string topicName, string subscriptionName, bool isSessionRequired)
        {
            var cleansedTopicName = CreateTopicName(topicName);
            var cleansedSubscriptionName = CreateSubscriptionName(subscriptionName);

            try
            {
                var subscription = await _manager.GetSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName);
                if (subscription.RequiresSession == !isSessionRequired)
                {
                    // the event has changed from/to an aggregate event, so the subscription needs to be removed and re-added to set the session flag
                    await _manager.DeleteSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName);
                    subscription.RequiresSession = isSessionRequired;
                    await _manager.CreateSubscriptionAsync(subscription);
                }
            }
            catch (MessagingEntityNotFoundException)
            {
                var sd = new SubscriptionDescription(cleansedTopicName, cleansedSubscriptionName);
                sd.RequiresSession = true;
                await _manager.CreateSubscriptionAsync(sd);
            }

            return new SubscriptionClient(_settings.AzureServiceBusConnectionString, cleansedTopicName, cleansedSubscriptionName);
        }        
    }
}