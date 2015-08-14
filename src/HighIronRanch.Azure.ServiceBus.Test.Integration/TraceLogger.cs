using System;
using System.Reflection;
using HighIronRanch.Core.Services;

namespace HighIronRanch.Azure.ServiceBus.Test.Integration
{
	public class TraceLogger : ILogger
	{
		public void Debug(string context, string message)
		{
			System.Diagnostics.Trace.WriteLine(context + ": " + message);
		}

		public void Debug(string context, string fmt, params object[] vars)
		{
			Debug(context, string.Format(fmt, vars));
		}

		public void Debug(string context, Exception exception, string fmt, params object[] vars)
		{
			Debug(context, "Exception: " + exception.ToString() + string.Format(fmt, vars));
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
			Debug(context, exception, fmt, vars);
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
			Debug(context, exception, fmt, vars);
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
			Debug(context, exception, fmt, vars);
		}

		public void TraceApi(string context, string componentName, string method, TimeSpan timespan)
		{
			var msg = string.Format("{0}: {1}.{2} {3}", context, componentName, method, timespan);
			System.Diagnostics.Trace.WriteLine(msg);
		}

		public void TraceApi(string context, string componentName, string method, TimeSpan timespan, string properties)
		{
			var msg = string.Format("{0}: {1}.{2} {3} - {4}", context, componentName, method, timespan, properties);
			System.Diagnostics.Trace.WriteLine(msg);
		}

		public void TraceApi(string context, string componentName, string method, TimeSpan timespan, string fmt, params object[] vars)
		{
			var msg = string.Format("{0}: {1}.{2} {3} - {4}", context, componentName, method, timespan, string.Format(fmt, vars));
			System.Diagnostics.Trace.WriteLine(msg);
		}

		public void TraceApi(string context, MethodBase methodBase, TimeSpan timespan, string message)
		{
			var msg = string.Format("{0}: {1} {2} - {3}", context, methodBase, timespan, message);
			System.Diagnostics.Trace.WriteLine(msg);
		}

		public void TraceApi(string context, MethodBase methodBase, TimeSpan timespan, string fmt, params object[] vars)
		{
			var msg = string.Format("{0}: {1} {2} - {3}", context, methodBase, timespan, string.Format(fmt, vars));
			System.Diagnostics.Trace.WriteLine(msg);
		}
	}
}