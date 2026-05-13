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

using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Cluster.Client;
using FakeItEasy;
using NUnit.Framework;
using AeronType = Adaptive.Aeron.Aeron;

namespace Adaptive.Cluster.Tests.Client
{
    public class AeronClusterContextTest
    {
        private AeronType _aeron;
        private AeronCluster.Context _context;

        [SetUp]
        public void Before()
        {
            _aeron = A.Fake<AeronType>();

            _context = new AeronCluster.Context()
                .AeronClient(_aeron)
                .IngressChannel("aeron:udp")
                .EgressChannel("aeron:udp?endpoint=localhost:0");
        }

        [TestCase(null)]
        [TestCase("")]
        public void ConcludeThrowsConfigurationExceptionIfIngressChannelIsNotSet(string ingressChannel)
        {
            _context.IngressChannel(ingressChannel);

            var exception = Assert.Throws<ConfigurationException>(() => _context.Conclude());
            Assert.AreEqual("ingressChannel must be specified", exception.Message);
        }

        [Test]
        public void ConcludeThrowsConfigurationExceptionIfIngressChannelIsSetToIpcAndIngressEndpointsSpecified()
        {
            _context.IngressChannel("aeron:ipc").IngressEndpoints("0,localhost:1234");

            var exception = Assert.Throws<ConfigurationException>(() => _context.Conclude());
            Assert.AreEqual(
                "AeronCluster.Context ingressEndpoints must be null when using IPC ingress",
                exception.Message
            );
        }

        [TestCase(null)]
        [TestCase("")]
        public void ConcludeThrowsConfigurationExceptionIfEgressChannelIsNotSet(string egressChannel)
        {
            _context.EgressChannel(egressChannel);

            var exception = Assert.Throws<ConfigurationException>(() => _context.Conclude());
            Assert.AreEqual("egressChannel must be specified", exception.Message);
        }

        [TestCase(null)]
        [TestCase("")]
        public void ClientNameShouldHandleEmptyValue(string clientName)
        {
            _context.ClientName(clientName);
            Assert.AreEqual("", _context.ClientName());
        }

        [TestCase("test")]
        [TestCase("Some other name")]
        public void ClientNameShouldReturnAssignedValue(string clientName)
        {
            _context.ClientName(clientName);
            Assert.AreEqual(clientName, _context.ClientName());
        }

        [TestCase("some")]
        [TestCase("42")]
        public void ClientNameCanBeSetViaSystemProperty(string clientName)
        {
            Config.Params[AeronCluster.Configuration.CLIENT_NAME_PROP_NAME] = clientName;
            try
            {
                Assert.AreEqual(clientName, new AeronCluster.Context().ClientName());
            }
            finally
            {
                Config.Params.Remove(AeronCluster.Configuration.CLIENT_NAME_PROP_NAME);
            }
        }

        [Test]
        public void ClientNameMustNotExceedMaxLength()
        {
            _context.ClientName("test" + new string('x', AeronType.Configuration.MAX_CLIENT_NAME_LENGTH));

            var exception = Assert.Throws<ConfigurationException>(() => _context.Conclude());
            Assert.AreEqual(
                "AeronCluster.Context.clientName length must be <= " + AeronType.Configuration.MAX_CLIENT_NAME_LENGTH,
                exception.Message
            );
        }
    }
}
