using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class BufferBuilderUtilTest
    {
        [Test]
        public void ShouldFindMaxCapacityWhenRequested()
        {
            
            Assert.That(BufferBuilderUtil.FindSuitableCapacity(0, BufferBuilderUtil.MAX_CAPACITY), 
                Is.EqualTo(BufferBuilderUtil.MAX_CAPACITY));
        }
    }
}