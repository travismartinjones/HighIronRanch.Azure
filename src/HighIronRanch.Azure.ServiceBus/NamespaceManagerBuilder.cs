using System;
using Microsoft.ServiceBus;

namespace HighIronRanch.Azure.ServiceBus
{
	public interface INamespaceManagerBuilder
	{
		NamespaceManagerBuilder CreateNamespaceBuilder();
		NamespaceManagerBuilder WithConnectionString(string connectionString);
		bool IsValid();
		NamespaceManager Build();
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

		public NamespaceManager Build()
		{
			if(IsValid())
				return NamespaceManager.CreateFromConnectionString(_connectionString);

			throw new InvalidOperationException("Invalid configuration");
		}
	}
}