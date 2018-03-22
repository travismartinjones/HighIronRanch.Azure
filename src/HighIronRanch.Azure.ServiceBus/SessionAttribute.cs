using System;

namespace HighIronRanch.Azure.ServiceBus
{
    [AttributeUsage(AttributeTargets.Class)]
    public class SessionAttribute : Attribute
    {
        public int TimeoutSeconds { get; }

        public SessionAttribute(int timeoutSeconds)
        {
            TimeoutSeconds = timeoutSeconds;
        }
    }
}