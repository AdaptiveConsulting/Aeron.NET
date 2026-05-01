using NUnit.Framework;
using Adaptive.Agrona.Util;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona;
using System.IO;

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
                Path.Combine(aeronDir, "cnc.dat"), MapMode.ReadOnly);

            UnsafeBuffer metadataBuffer = CncFileDescriptor.CreateMetaDataBuffer(cncByteBuffer);

            UnsafeBuffer countersValueBuffer = CncFileDescriptor.CreateCountersValuesBuffer(cncByteBuffer, metadataBuffer);
            UnsafeBuffer countersMetadataBuffer = CncFileDescriptor.CreateCountersMetaDataBuffer(cncByteBuffer, metadataBuffer);

            Assert.AreEqual(Tests.countersMetadataBufferLength(countersValueBuffer.Capacity), countersMetadataBuffer.Capacity);
        }
    }
}