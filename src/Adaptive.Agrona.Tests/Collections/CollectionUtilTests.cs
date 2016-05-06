using System.Collections.Generic;
using Adaptive.Agrona.Collections;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Collections
{
    [TestFixture]
    public class CollectionUtilTest
    {
        [Test]
        public void GetOrDefaultUsesSupplier()
        {
            var ints = new Dictionary<int?, int?>();
            var result = CollectionUtil.GetOrDefault(ints, 0, x => x + 1);

            Assert.That(result, Is.EqualTo(1));
        }

        [Test]
        public void GetOrDefaultDoesNotCreateNewValueWhenOneExists()
        {
            var ints = new Dictionary<int?, int?> {[0] = 0};
            var result = CollectionUtil.GetOrDefault(ints, 0, (x) =>
            {
                Assert.Fail("Shouldn't be called");
                return x + 1;
            });

            Assert.That(result, Is.EqualTo(0));
        }
    }
}