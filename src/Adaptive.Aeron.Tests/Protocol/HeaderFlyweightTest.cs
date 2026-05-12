/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
