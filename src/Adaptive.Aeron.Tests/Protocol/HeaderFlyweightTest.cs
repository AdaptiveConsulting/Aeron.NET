using System.Text;
using Adaptive.Aeron.Protocol;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.Protocol
{
    [TestFixture]
    public class HeaderFlyweightTest
    {
        [Test]
        public void ShouldConvertFlags()
        {
            short flags = 0b01101000;

            char[] flagsAsChars = HeaderFlyweight.FlagsToChars(flags);

            Assert.That(flagsAsChars, Is.EqualTo("01101000"));
        }

        [Test]
        public void ShouldAppendFlags()
        {
            short flags = 0b01100000;
            StringBuilder builder = new StringBuilder();

            HeaderFlyweight.AppendFlagsAsChars(flags, builder);

            Assert.That(builder.ToString(), Is.EqualTo("01100000"));
        }
    }
}