using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;
using static Adaptive.Aeron.LogBuffer.LogBufferDescriptor;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    public class LogBufferDescriptorTest
    {
        private readonly UnsafeBuffer _metadataBuffer = new UnsafeBuffer(new byte[LOG_META_DATA_LENGTH]);

        [Test]
        public void RotateLogShouldCasActiveTermCountEvenWhenTermIdDoesNotMatch()
        {
            const int termId = 5;
            const int termCount = 1;
            RawTail(_metadataBuffer, 2, PackTail(termId, 1024));
            RawTail(_metadataBuffer, 0, PackTail(termId + 1, 2048));
            RawTail(_metadataBuffer, 1, PackTail(termId + 2, 4096));
            ActiveTermCount(_metadataBuffer, termCount);

            Assert.IsTrue(RotateLog(_metadataBuffer, termCount, termId));

            Assert.AreEqual(PackTail(termId, 1024), RawTail(_metadataBuffer, 2));
            Assert.AreEqual(PackTail(termId + 1, 2048), RawTail(_metadataBuffer, 0));
            Assert.AreEqual(PackTail(termId + 2, 4096), RawTail(_metadataBuffer, 1));
            Assert.AreEqual(termCount + 1, ActiveTermCount(_metadataBuffer));
        }

        [Test]
        public void RotateLogShouldCasActiveTermCountAfterSettingTailForTheNextTerm()
        {
            const int termId = 51;
            const int termCount = 19;
            RawTail(_metadataBuffer, 1, PackTail(termId, 1024));
            RawTail(_metadataBuffer, 2, PackTail(termId + 1 - PARTITION_COUNT, 2048));
            RawTail(_metadataBuffer, 0, PackTail(termId + 2 - PARTITION_COUNT, 4096));
            ActiveTermCount(_metadataBuffer, termCount);

            Assert.IsTrue(RotateLog(_metadataBuffer, termCount, termId));

            Assert.AreEqual(PackTail(termId, 1024), RawTail(_metadataBuffer, 1));
            Assert.AreEqual(PackTail(termId + 1, 0), RawTail(_metadataBuffer, 2));
            Assert.AreEqual(PackTail(termId + 2 - PARTITION_COUNT, 4096), RawTail(_metadataBuffer, 0));
            Assert.AreEqual(termCount + 1, ActiveTermCount(_metadataBuffer));
        }

        [Test]
        public void RotateLogIsANoOpIfNeitherTailNorActiveTermCountCanBeChanged()
        {
            const int termId = 23;
            const int termCount = 42;
            RawTail(_metadataBuffer, 0, PackTail(termId, 1024));
            RawTail(_metadataBuffer, 1, PackTail(termId + 18, 2048));
            RawTail(_metadataBuffer, 2, PackTail(termId - 19, 4096));
            ActiveTermCount(_metadataBuffer, termCount);

            Assert.IsFalse(RotateLog(_metadataBuffer, 3, termId));

            Assert.AreEqual(PackTail(termId, 1024), RawTail(_metadataBuffer, 0));
            Assert.AreEqual(PackTail(termId + 18, 2048), RawTail(_metadataBuffer, 1));
            Assert.AreEqual(PackTail(termId - 19, 4096), RawTail(_metadataBuffer, 2));
            Assert.AreEqual(termCount, ActiveTermCount(_metadataBuffer));
        }

        [TestCase(0, 1376, 0)]
        [TestCase(10, 1024, 64)]
        [TestCase(2048, 2048, 2080)]
        [TestCase(4096, 1024, 4224)]
        [TestCase(7997, 992, 8288)]
        public void ShouldComputeFragmentedFrameLength(int length, int maxPayloadLength, int frameLength)
        {
            Assert.AreEqual(LogBufferDescriptor.ComputeFragmentedFrameLength(length, maxPayloadLength), frameLength);
        }
    }
}
