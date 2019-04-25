using System;

namespace HighIronRanch.Azure.ServiceBus
{
    public class EnqueueOptions
    {
        /// <summary>
        /// If set and EnqueueTime is set, for any future schedule messages, will look forward N seconds to determine if a
        /// message of the same type is already scheduled. If so, the enqueue request will be discarded.
        /// Likewise, if a message will process N seconds after the message is scheduled to enqueue, it
        /// will also be discarded.
        /// </summary>
        public int? DuplicatePreventionSeconds { get; set; }
        /// <summary>
        /// If set, will schedule the message to be processed in the future. If the date less than 2 seconds,
        /// from now, it will be enqueued immediately to avoid the message being rejected by azure service bus.
        /// </summary>
        public DateTime? EnqueueTime { get; set; }
        /// <summary>
        /// If true and EnqueueTime has a value, then any messages that already exist of the same type will be cancelled
        /// and removed from the queue.
        /// </summary>
        public bool RemoveAnyExisting { get; set; }
        /// <summary>
        /// If set, will remove all messages that already exist of the same type and cancels them, with the exception of
        /// the item that will dequeue next within the window specified by the value. The window is defined by the EnqueueTime
        /// and N seconds after the EnqueueTime.
        /// </summary>
        public int? RemoveAllButLastInWindowSeconds { get; set; }
    }
}