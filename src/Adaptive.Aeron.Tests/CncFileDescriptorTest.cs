using NUnit.Framework;
using Adaptive.Agrona.Util;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona;
using System.IO;

namespace Adaptive.Aeron.Tests
{
    class CncFileDescriptorTest
    {
        [Test]
        [Ignore("Media driver needs to be running")]
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