using Adaptive.Agrona.Collections;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Collections
{
    [TestFixture]
    public class ArrayUtilTest
    {
        // Reference Equality
        private static readonly object One = new object();
        private static readonly object Two = new object();
        private static readonly object Three = new object();

        private readonly object[] _values = { One, Two };

        [Test]
        public void ShouldNotRemoveMissingElement()
        {
            var result = ArrayUtil.Remove(_values, Three);

            Assert.That(_values, Is.EqualTo(result));
        }

        [Test]
        public void ShouldRemovePresentElementAtEnd()
        {
            var result = ArrayUtil.Remove(_values, Two);

            Assert.That(new[] { One }, Is.EqualTo(result));
        }

        [Test]
        public void ShouldRemovePresentElementAtStart()
        {
            var result = ArrayUtil.Remove(_values, One);

            Assert.That(new [] { Two }, Is.EqualTo(result));
        }

        [Test]
        public void ShouldRemoveByIndex()
        {
            var result = ArrayUtil.Remove(_values, 0);

            Assert.That(new [] { Two }, Is.EqualTo(result));
        }
    }

}