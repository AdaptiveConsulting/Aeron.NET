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