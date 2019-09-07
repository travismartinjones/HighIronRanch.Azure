using System;

namespace HighIronRanch.Azure.ServiceBus
{
    [AttributeUsage(AttributeTargets.Class)]
    public class SessionAttribute : Attribute
    {
        public int TimeoutSeconds { get; }
        private static readonly TimeSpan _defaultSessionWaitTime = new TimeSpan(0, 0, 30);

        public SessionAttribute(int timeoutSeconds)
        {
            TimeoutSeconds = timeoutSeconds;
        }

        public static TimeSpan GetWaitTimeForType(Type messageType)
        {
            var sessionAttribute = (SessionAttribute)Attribute.GetCustomAttribute(messageType, typeof(SessionAttribute));
            if (sessionAttribute == null)
                return _defaultSessionWaitTime;
            return new TimeSpan(0, 0, sessionAttribute.TimeoutSeconds);
        }
    }
}