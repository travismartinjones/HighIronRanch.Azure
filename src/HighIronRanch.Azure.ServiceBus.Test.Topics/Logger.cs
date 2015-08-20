using System;
using System.Reflection;
using HighIronRanch.Core.Services;

namespace HighIronRanch.Azure.ServiceBus.Test.Topics
{
	public class Logger : ILogger
	{
		public void Debug(string context, string message)
		{
			Console.WriteLine("{0} | {1}", context, message);
		}

		public void Debug(string context, string fmt, params object[] vars)
		{
			Debug(context, string.Format(fmt, vars));
		}

		public void Debug(string context, Exception exception, string fmt, params object[] vars)
		{
			Debug(context, string.Format("{0} | {1}", exception.ToString(), string.Format(fmt, vars)));
		}

		public void Information(string context, string message)
		{
			Debug(context, message);
		}

		public void Information(string context, string fmt, params object[] vars)
		{
			Debug(context, string.Format(fmt, vars));
		}

		public void Information(string context, Exception exception, string fmt, params object[] vars)
		{
			Debug(context, string.Format("{0} | {1}", exception.ToString(), string.Format(fmt, vars)));
		}

		public void Warning(string context, string message)
		{
			Debug(context, message);
		}

		public void Warning(string context, string fmt, params object[] vars)
		{
			Debug(context, string.Format(fmt, vars));
		}

		public void Warning(string context, Exception exception, string fmt, params object[] vars)
		{
			Debug(context, string.Format("{0} | {1}", exception.ToString(), string.Format(fmt, vars)));
		}

		public void Error(string context, string message)
		{
			Debug(context, message);
		}

		public void Error(string context, string fmt, params object[] vars)
		{
			Debug(context, string.Format(fmt, vars));
		}

		public void Error(string context, Exception exception, string fmt, params object[] vars)
		{
			Debug(context, string.Format("{0} | {1}", exception.ToString(), string.Format(fmt, vars)));
		}

		public void TraceApi(string context, string componentName, string method, TimeSpan timespan)
		{
			Debug(context, string.Format("{0} {1} {2}", componentName, method, timespan));
		}

		public void TraceApi(string context, string componentName, string method, TimeSpan timespan, string properties)
		{
			Debug(context, string.Format("{0} {1} {2} {3}", componentName, method, timespan, properties));
		}

		public void TraceApi(string context, string componentName, string method, TimeSpan timespan, string fmt, params object[] vars)
		{
			Debug(context, string.Format("{0} {1} {2} {3}", componentName, method, timespan, string.Format(fmt, vars)));
		}

		public void TraceApi(string context, MethodBase methodBase, TimeSpan timespan, string message)
		{
			Debug(context, string.Format("{0} {1} {2}", methodBase, timespan, message));
		}

		public void TraceApi(string context, MethodBase methodBase, TimeSpan timespan, string fmt, params object[] vars)
		{
			Debug(context, string.Format("{0} {1} {2}", methodBase, timespan, string.Format(fmt, vars)));
		}
	}
}