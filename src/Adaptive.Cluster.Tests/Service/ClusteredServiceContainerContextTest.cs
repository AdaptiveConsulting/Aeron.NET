/*
 * Copyright 2026 Adaptive Financial Consulting Ltd
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
using System.IO;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Cluster.Service;
using FakeItEasy;
using NUnit.Framework;
using AeronType = Adaptive.Aeron.Aeron;

namespace Adaptive.Cluster.Tests.Service
{
    public class ClusteredServiceContainerContextTest
    {
        private ClusteredServiceContainer.Context _context;
        private DirectoryInfo _clusterDir;

        [SetUp]
        public void Before()
        {
            _clusterDir = new DirectoryInfo(
                Path.Combine(Path.GetTempPath(), "cluster-ctx-test-" + Guid.NewGuid()));

            AeronType aeron = A.Fake<AeronType>();
            AeronType.Context aeronCtx = A.Fake<AeronType.Context>();
            A.CallTo(() => aeronCtx.AeronDirectoryName()).Returns("test-aeron-dir");
            A.CallTo(() => aeronCtx.SubscriberErrorHandler()).Returns(RethrowingErrorHandler.INSTANCE);
            A.CallTo(() => aeronCtx.FilePageSize()).Returns(LogBufferDescriptor.PAGE_MIN_SIZE);
            A.CallTo(() => aeron.Ctx).Returns(aeronCtx);

            UnsafeBuffer metaDataBuffer = new UnsafeBuffer(new byte[128 * 1024]);
            UnsafeBuffer valuesBuffer = new UnsafeBuffer(new byte[64 * 1024]);
            CountersManager countersManager = new CountersManager(metaDataBuffer, valuesBuffer);

            A.CallTo(() => aeron.AddCounter(A<int>._, A<IDirectBuffer>._, A<int>._, A<int>._, A<IDirectBuffer>._,
                    A<int>._, A<int>._))
                .ReturnsLazily((int typeId, IDirectBuffer kb, int ko, int kl, IDirectBuffer lb, int lo, int ll) =>
                    new Counter(countersManager, countersManager.Allocate("my-counter", typeId)));

            _context = new ClusteredServiceContainer.Context()
                .AeronClient(aeron)
                .ClusterDir(_clusterDir)
                .ServiceId(0)
                .ClusteredService(A.Fake<IClusteredService>());
        }

        [TearDown]
        public void After()
        {
            _context?.Dispose();
            _context?.DeleteDirectory();
        }

        [Test]
        public void ShouldUseDefaultVersionValidatorWhenNoneSuppliedToContext()
        {
            _context.Conclude();

            Assert.That(_context.AppVersionValidator(), Is.SameAs(AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR));
        }

        [Test]
        public void ShouldPreserveCustomVersionValidatorSuppliedToContextThroughConclude()
        {
            var customValidator = A.Fake<IVersionValidator>();
            _context.AppVersionValidator(customValidator);

            _context.Conclude();

            Assert.That(_context.AppVersionValidator(), Is.SameAs(customValidator));
        }
    }
}
