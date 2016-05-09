using System;
using System.Collections.Generic;
using System.Linq;

namespace Adaptive.Aeron
{
    internal class ActiveSubscriptions : IDisposable
    {
        private readonly Dictionary<int, List<Subscription>> _subscriptionsByStreamIdMap = new Dictionary<int, List<Subscription>>();

        public void ForEach(int streamId, Action<Subscription> handler)
        {
            List<Subscription> subscriptions;
            if (_subscriptionsByStreamIdMap.TryGetValue(streamId, out subscriptions))
            {
                subscriptions.ForEach(handler);
            }
        }

        public void Add(Subscription subscription)
        {
            List<Subscription> subscriptions;
            if (!_subscriptionsByStreamIdMap.TryGetValue(subscription.StreamId(), out subscriptions))
            {
                subscriptions = new List<Subscription>();
                _subscriptionsByStreamIdMap[subscription.StreamId()] = subscriptions;
            }
            
            subscriptions.Add(subscription);
        }

        public void Remove(Subscription subscription)
        {
            int streamId = subscription.StreamId();
            var subscriptions = _subscriptionsByStreamIdMap[streamId];
            if (subscriptions.Remove(subscription) && subscriptions.Count == 0)
            {
                _subscriptionsByStreamIdMap.Remove(streamId);
            }
        }

        public void Dispose()
        {
            var subscriptions = from subs in _subscriptionsByStreamIdMap.Values
                from subscription in subs
                select subscription;

            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
        }
    }
}