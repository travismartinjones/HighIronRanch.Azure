using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
	public interface IServiceBusSettings
	{
		string AzureServiceBusConnectionString { get; }
	}

	public interface IServiceBus
	{
		Task CreateQueueAsync(string name, bool isSessionRequired);
		Task<QueueClient> CreateQueueClientAsync(string name);
		Task<QueueClient> CreateQueueClientAsync(string name, bool isSessionRequired);
		Task DeleteQueueAsync(string name);
		Task<TopicClient> CreateTopicClientAsync(string name);
		Task DeleteTopicAsync(string name);
		Task<SubscriptionClient> CreateSubscriptionClientAsync(string topicName, string subscriptionName);
		Task DeleteSubscriptionAsync(string topicName, string subscriptionName);
	}

	public class ServiceBus : IServiceBus
	{
		private readonly IServiceBusSettings _settings;
		protected NamespaceManager _manager;

		public ServiceBus(IServiceBusSettings settings, INamespaceManagerBuilder managerBuilder)
		{
			_settings = settings;

			_manager = managerBuilder
				.CreateNamespaceBuilder()
				.WithConnectionString(_settings.AzureServiceBusConnectionString)
				.Build();
		}

		public async Task CreateQueueAsync(string name, bool isSessionRequired)
		{
			var qd = new QueueDescription(name);
			qd.RequiresSession = isSessionRequired;
			qd.EnableDeadLetteringOnMessageExpiration = true;

			if (!_manager.QueueExists(name))
				await _manager.CreateQueueAsync(qd);
		}

		public async Task<QueueClient> CreateQueueClientAsync(string name)
		{
			return await CreateQueueClientAsync(name, false);
		}

		public async Task<QueueClient> CreateQueueClientAsync(string name, bool isSessionRequired)
		{
			await CreateQueueAsync(name, isSessionRequired);

			return QueueClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, name);
		}

		public async Task DeleteQueueAsync(string name)
		{
			await _manager.DeleteQueueAsync(name);
		}

		public async Task<TopicClient> CreateTopicClientAsync(string name)
		{
			var td = new TopicDescription(name);

			if (!_manager.TopicExists(name))
				await _manager.CreateTopicAsync(td);

			return TopicClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, name);
		}

		public async Task DeleteTopicAsync(string name)
		{
			await _manager.DeleteTopicAsync(name);
		}

		public async Task<SubscriptionClient> CreateSubscriptionClientAsync(string topicName, string subscriptionName)
		{
			var sd = new SubscriptionDescription(topicName, subscriptionName);

			if (!_manager.SubscriptionExists(topicName, subscriptionName))
				await _manager.CreateSubscriptionAsync(sd);

			return SubscriptionClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, topicName, subscriptionName);
		}

		public async Task DeleteSubscriptionAsync(string topicName, string subscriptionName)
		{
			await _manager.DeleteSubscriptionAsync(topicName, subscriptionName);
		}
	}
}