using NUnit.Framework;

namespace Adaptive.Agrona.Tests
{
    [TestFixture]
    public class TimeUnitTests
    {
        [Test]
        public void MillisToNanos()
        {
            Assert.AreEqual(1000000, TimeUnit.MILLIS.ToNanos(1));
        }
        
        [Test]
        public void NanosToMillis()
        {
            Assert.AreEqual(1, TimeUnit.NANOSECONDS.ToMillis(1000000));
        }
    }
}