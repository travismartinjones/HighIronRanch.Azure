using System;
using System.Linq.Expressions;

namespace HighIronRanch.Azure.ServiceBus
{
    [AttributeUsage(AttributeTargets.Class)]
    public class SessionAttribute : Attribute
    {
        public int[] DelayBetweenRetries { get; }
        public int TimeoutSeconds { get; }        

        public SessionAttribute(int timeoutSeconds, int[] delayBetweenRetries = null)
        {
            DelayBetweenRetries = delayBetweenRetries;
            TimeoutSeconds = timeoutSeconds;
        }

        public static TimeSpan GetWaitTimeForType(Type messageType, int defaultWaitSeconds)
        {
            var sessionAttribute = (SessionAttribute)Attribute.GetCustomAttribute(messageType, typeof(SessionAttribute));
            if (sessionAttribute == null)
                return new TimeSpan(0, 0, defaultWaitSeconds);

            return new TimeSpan(0, 0, sessionAttribute.TimeoutSeconds);
        }

        public static int GetDelayForType(Type messageType, int deliveryCount)
        {            
            var sessionAttribute = (SessionAttribute)Attribute.GetCustomAttribute(messageType, typeof(SessionAttribute));
            
            if (sessionAttribute?.DelayBetweenRetries != null && sessionAttribute?.DelayBetweenRetries.Length != 0)
                return sessionAttribute.DelayBetweenRetries[Math.Min(sessionAttribute.DelayBetweenRetries.Length - 1, deliveryCount)];

            switch (deliveryCount)
            {
                case 9:
                    return 1000;
                case 8:
                    return 500;
                case 7:
                    return 100;
                default:
                    return 50;
            }
        }
    }
}