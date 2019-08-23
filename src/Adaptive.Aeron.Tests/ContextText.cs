using Adaptive.Aeron.Exceptions;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    [TestFixture]
    public class ContextText
    {
        [ExpectedException(typeof(ConcurrentConcludeException))]
        public void ShouldNotAllowConcludeMoreThanOnce()
        {
            var ctx = new Aeron.Context();

            ctx.Conclude();
            ctx.Conclude();
        }
    }
}