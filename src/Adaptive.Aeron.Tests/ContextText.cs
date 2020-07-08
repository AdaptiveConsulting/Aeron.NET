using Adaptive.Aeron.Exceptions;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    [TestFixture]
    public class ContextText
    {
        [Test]
        [Ignore("Media driver needs to be running")]
        public void ShouldNotAllowConcludeMoreThanOnce()
        {
            var ctx = new Aeron.Context();

            ctx.Conclude();
            Assert.Throws(typeof(ConcurrentConcludeException), () => ctx.Conclude());
        }
        [Test]
        [Ignore("Media driver needs to be running")]
        public void ShouldAllowConcludeOfClonedContext()
        {
            var ctx = new Aeron.Context();

            var ctx2 = ctx.Clone();

            ctx.Conclude();
            ctx2.Conclude();
        }
    }
}