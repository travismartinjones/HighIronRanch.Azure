using System;

namespace HighIronRanch.Azure.ServiceBus
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TopicNameAttribute : Attribute
    {
        public string Name { get; }

        public TopicNameAttribute(string name)
        {
            Name = name;
        }

        public static string GetTopicNameForType(Type messageType)
        {
            var nameAttribute = (TopicNameAttribute)Attribute.GetCustomAttribute(messageType, typeof(TopicNameAttribute));
            return nameAttribute == null ? messageType.FullName : nameAttribute.Name;
        }
    }
}