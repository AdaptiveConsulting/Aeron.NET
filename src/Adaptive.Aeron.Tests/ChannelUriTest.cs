/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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

using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class ChannelUriTest
    {
        [Test]
        public void ShouldSubstituteEndpoint()
        {
            AssertSubstitution("aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:0", "localhost:12345");
            AssertSubstitution("aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:12345", "localhost:54321");
            AssertSubstitution("aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:0", "127.0.0.1:12345");
            AssertSubstitution("aeron:udp?endpoint=127.0.0.1:12345", "aeron:udp", "127.0.0.1:12345");
        }

        [Test]
        public void ShouldThrowIfResolvedEndpointInvalid()
        {
            ChannelUri uri = ChannelUri.Parse("aeron:udp?endpoint=localhost:0");
            
            Assert.Throws(typeof(ArgumentException), () => uri.ReplaceEndpointWildcardPort("localhost:0"));
            Assert.Throws(typeof(ArgumentException), () => uri.ReplaceEndpointWildcardPort("localhost"));
            Assert.Throws(typeof(ArgumentNullException), () => uri.ReplaceEndpointWildcardPort(null));
        }

        private static void AssertSubstitution(string expected, string originalChannel, string resolvedEndpoint)
        {
            ChannelUri uri = ChannelUri.Parse(originalChannel);
            uri.ReplaceEndpointWildcardPort(resolvedEndpoint);
            Assert.AreEqual(expected, uri.ToString());
        }
    }
}