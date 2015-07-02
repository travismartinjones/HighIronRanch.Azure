using System.Text.RegularExpressions;
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

		protected static string CleanseName(string name)
		{
			// Entity segments can contain only letters, numbers, periods (.), hyphens (-), and underscores (_)
			return Regex.Replace(name, @"[^a-zA-Z0-9\.\-_]", "_");
		}

		public async Task CreateQueueAsync(string name, bool isSessionRequired)
		{
			var cleansedName = CleanseName(name);
			await CreateCleansedNameQueueAsync(cleansedName, isSessionRequired);
		}

		private async Task CreateCleansedNameQueueAsync(string cleansedName, bool isSessionRequired)
		{
			var qd = new QueueDescription(cleansedName);
			qd.RequiresSession = isSessionRequired;
			qd.EnableDeadLetteringOnMessageExpiration = true;

			if (!_manager.QueueExists(cleansedName))
				await _manager.CreateQueueAsync(qd);
		}

		public async Task<QueueClient> CreateQueueClientAsync(string name)
		{
			return await CreateQueueClientAsync(name, false);
		}

		public async Task<QueueClient> CreateQueueClientAsync(string name, bool isSessionRequired)
		{
			var cleansedName = CleanseName(name);
			await CreateCleansedNameQueueAsync(cleansedName, isSessionRequired);

			return QueueClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, cleansedName);
		}

		public async Task DeleteQueueAsync(string name)
		{
			var cleansedName = CleanseName(name);
			await _manager.DeleteQueueAsync(cleansedName);
		}

		public async Task<TopicClient> CreateTopicClientAsync(string name)
		{
			var cleansedName = CleanseName(name);
			var td = new TopicDescription(cleansedName);

			if (!_manager.TopicExists(cleansedName))
				await _manager.CreateTopicAsync(td);

			return TopicClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, cleansedName);
		}

		public async Task DeleteTopicAsync(string name)
		{
			var cleansedName = CleanseName(name);
			await _manager.DeleteTopicAsync(cleansedName);
		}

		public async Task<SubscriptionClient> CreateSubscriptionClientAsync(string topicName, string subscriptionName)
		{
			var cleansedTopicName = CleanseName(topicName);
			var cleansedSubscriptionName = CleanseName(subscriptionName);
			var sd = new SubscriptionDescription(cleansedTopicName, cleansedSubscriptionName);

			if (!_manager.SubscriptionExists(cleansedTopicName, subscriptionName))
				await _manager.CreateSubscriptionAsync(sd);

			return SubscriptionClient.CreateFromConnectionString(_settings.AzureServiceBusConnectionString, cleansedTopicName, cleansedSubscriptionName);
		}

		public async Task DeleteSubscriptionAsync(string topicName, string subscriptionName)
		{
			var cleansedTopicName = CleanseName(topicName);
			var cleansedSubscriptionName = CleanseName(subscriptionName);
			await _manager.DeleteSubscriptionAsync(cleansedTopicName, cleansedSubscriptionName);
		}
	}
}