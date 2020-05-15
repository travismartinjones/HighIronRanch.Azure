using System;

namespace HighIronRanch.Azure.ServiceBus
{
    [AttributeUsage(AttributeTargets.Class)]
    public class QueueNameAttribute : Attribute
    {
        public string Name { get; }

        public QueueNameAttribute(string name)
        {
            Name = name;
        }

        public static string GetQueueNameForType(Type messageType)
        {
            var nameAttribute = (QueueNameAttribute)Attribute.GetCustomAttribute(messageType, typeof(QueueNameAttribute));
            return nameAttribute == null ? messageType.FullName : nameAttribute.Name;
        }
    }
}