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

using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    class CncFileDescriptorTest
    {
        private EmbeddedMediaDriver _driver;

        [SetUp]
        public void StartDriver() => _driver = new EmbeddedMediaDriver();

        [TearDown]
        public void StopDriver() => _driver?.Dispose();

        [Test]
        public void ShouldAllocateCapacityForCounterMetadataBuffer()
        {
            string aeronDir = Aeron.Context.GetAeronDirectoryName();

            MappedByteBuffer cncByteBuffer = IoUtil.MapExistingFile(
                Path.Combine(aeronDir, "cnc.dat"),
                MapMode.ReadOnly
            );

            UnsafeBuffer metadataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);

            UnsafeBuffer countersValueBuffer = CncFileDescriptor.CreateCountersValuesBuffer(
                cncByteBuffer,
                metadataBuffer
            );
            UnsafeBuffer countersMetadataBuffer = CncFileDescriptor.CreateCountersMetaDataBuffer(
                cncByteBuffer,
                metadataBuffer
            );

            Assert.AreEqual(
                Tests.CountersMetadataBufferLength(countersValueBuffer.Capacity),
                countersMetadataBuffer.Capacity
            );
        }
    }
}
