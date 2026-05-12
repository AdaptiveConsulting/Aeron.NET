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

using System;
using System.Linq.Expressions;
using FakeItEasy;
using FakeItEasy.Configuration;

namespace Adaptive.Agrona.Tests.Concurrent.RingBuffer
{
    public class InOrder
    {
        private UnorderedCallAssertion _firstUnorderedCall;
        private IOrderableCallAssertion _lastOrderCall;

        public void CallTo(Expression<Action> p0)
        {
            var assertion = A.CallTo(p0).MustHaveHappened();

            if (_firstUnorderedCall == null)
            {
                _firstUnorderedCall = assertion;
            }
            else if (_lastOrderCall == null)
            {
                _lastOrderCall = _firstUnorderedCall.Then(assertion);
            }
            else
            {
                _lastOrderCall = _lastOrderCall.Then(assertion);
            }
        }
    }
}
