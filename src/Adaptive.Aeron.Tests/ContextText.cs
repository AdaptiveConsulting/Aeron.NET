using Adaptive.Aeron.Exceptions;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    [TestFixture]
    public class ContextText
    {
        private EmbeddedMediaDriver _driver;

        [SetUp]
        public void StartDriver() => _driver = new EmbeddedMediaDriver();

        [TearDown]
        public void StopDriver() => _driver?.Dispose();

        [Test]
        public void ShouldNotAllowConcludeMoreThanOnce()
        {
            var ctx = new Aeron.Context();

            ctx.Conclude();
            Assert.Throws(typeof(ConcurrentConcludeException), () => ctx.Conclude());
        }
        [Test]
        public void ShouldAllowConcludeOfClonedContext()
        {
            var ctx = new Aeron.Context();

            var ctx2 = ctx.Clone();

            ctx.Conclude();
            ctx2.Conclude();
        }
    }
}