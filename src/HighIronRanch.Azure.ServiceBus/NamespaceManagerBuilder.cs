using System;
using Microsoft.Azure.ServiceBus.Management;

namespace HighIronRanch.Azure.ServiceBus
{
	public interface INamespaceManagerBuilder
	{
		NamespaceManagerBuilder CreateNamespaceBuilder();
		NamespaceManagerBuilder WithConnectionString(string connectionString);
		bool IsValid();
        ManagementClient  Build();
	}

	public class NamespaceManagerBuilder : INamespaceManagerBuilder
	{
		public NamespaceManagerBuilder CreateNamespaceBuilder()
		{
			return this;
		}

		protected string _connectionString;

		public NamespaceManagerBuilder WithConnectionString(string connectionString)
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

		public ManagementClient  Build()
		{
			if(IsValid())
				return new ManagementClient(_connectionString);

			throw new InvalidOperationException("Invalid configuration");
		}
	}
}