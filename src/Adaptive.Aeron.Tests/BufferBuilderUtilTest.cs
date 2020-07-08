using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class BufferBuilderUtilTest
    {
        [Test]
        public void ShouldFindMaxCapacityWhenRequested()
        {
            
            Assert.AreEqual(BufferBuilderUtil.MAX_CAPACITY, 
                BufferBuilderUtil.FindSuitableCapacity(0, BufferBuilderUtil.MAX_CAPACITY));
        }
    }
}