using NUnit.Framework;

namespace Adaptive.Aeron.Samples.Common.Tests
{
    [TestFixture]
    public class ComputerSpecificationsTest
    {
        [Test]
        public void CallingDumpWorks()
        {
            ComputerSpecifications.Dump();
        }

        [Test]
        [Ignore("Doesn't work on all machines.")]
        public void MemoryIsNonZero()
        {
            var c = new ComputerSpecifications();
            Assert.NotZero(c.MemoryMBytes);
        }
    }
}