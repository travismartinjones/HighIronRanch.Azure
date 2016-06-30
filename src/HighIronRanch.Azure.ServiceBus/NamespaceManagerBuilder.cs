using System;
using System.ServiceModel.Channels;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace HighIronRanch.Azure.ServiceBus
{
	public interface INamespaceManagerBuilder
	{
        INamespaceManagerBuilder CreateNamespaceBuilder();
        INamespaceManagerBuilder WithConnectionString(string connectionString);
		bool IsValid();
		NamespaceManager Build();
	}

	public class NamespaceManagerBuilder : INamespaceManagerBuilder
	{
		public INamespaceManagerBuilder CreateNamespaceBuilder()
		{
			return this;
		}

		protected string _connectionString;

		public INamespaceManagerBuilder WithConnectionString(string connectionString)
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

		public NamespaceManager Build()
		{
			if(IsValid())
				return NamespaceManager.CreateFromConnectionString(_connectionString);

			throw new InvalidOperationException("Invalid configuration");
		}
	}
}