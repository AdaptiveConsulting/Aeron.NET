/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            if (!_subscriptionsByStreamIdMap.TryGetValue(subscription.StreamId, out subscriptions))
            {
                subscriptions = new List<Subscription>();
                _subscriptionsByStreamIdMap[subscription.StreamId] = subscriptions;
            }
            
            subscriptions.Add(subscription);
        }

        public void Remove(Subscription subscription)
        {
            int streamId = subscription.StreamId;

            List<Subscription> subscriptions;
            if (_subscriptionsByStreamIdMap.TryGetValue(streamId, out subscriptions))
            {
                if (subscriptions.Remove(subscription) && subscriptions.Count == 0)
                {
                    _subscriptionsByStreamIdMap.Remove(streamId);
                }
            }
        }

        public void Dispose()
        {
            var subscriptions = from subs in _subscriptionsByStreamIdMap.Values
                from subscription in subs
                select subscription;

            foreach (var subscription in subscriptions)
            {
                subscription.Release();
            }
        }
    }
}