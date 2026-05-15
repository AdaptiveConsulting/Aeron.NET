/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Adaptive.Aeron.LogBuffer;
using NUnit.Framework;

namespace Adaptive.Archiver.IntegrationTests
{
    [TestFixture]
    internal class ControlledPollingPersistentSubscriptionTest : PersistentSubscriptionTest
    {
        protected override int Poll(PersistentSubscription subscription, IFragmentHandler handler, int fragmentLimit)
        {
            var controlledHandler = handler == null ? null : new ControlledFragmentHandlerAdapter(handler);
            return subscription.ControlledPoll(controlledHandler, fragmentLimit);
        }

        private sealed class ControlledFragmentHandlerAdapter : IControlledFragmentHandler
        {
            private readonly IFragmentHandler _inner;

            public ControlledFragmentHandlerAdapter(IFragmentHandler inner)
            {
                _inner = inner;
            }

            public ControlledFragmentHandlerAction OnFragment(
                Adaptive.Agrona.IDirectBuffer buffer, int offset, int length, Header header)
            {
                _inner.OnFragment(buffer, offset, length, header);
                return ControlledFragmentHandlerAction.CONTINUE;
            }
        }
    }
}
