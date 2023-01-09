using System.IO;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent.Status;
using static Adaptive.Aeron.LogBuffer.LogBufferDescriptor;

namespace Adaptive.Aeron.Tests
{
    /*
     
     TODO create log buffers from Client side (no raw factory)
     How to create temporary directory in NUnit?
     
    public class ImageRangeTest
    {
        public void ShouldHandleAllPossibleOffsets(bool useSpareFiles, File baseDir)
        {
            const int termBufferLength = 65536;
            const int filePageSize = 4096;
            const long subscriberPositionThatWillTriggerException = 3147497471L;
            var subscriberPosition = new AtomicLongPosition();

            using (FileStoreLogFactory fileStoreLogFactory = new FileStoreLogFactory(baseDir.GetAbsolutePath(),
                       filePageSize, false, 0, new RethrowingErrorHandler()))
            using (RawLog rawLog = fileStoreLogFactory.NewImage(0, termBufferLength, useSpareFiles))
            {
                InitialTermId(rawLog.MetaData(), 0);
                MtuLength(rawLog.MetaData(), 1408);
                TermLength(rawLog.MetaData(), termBufferLength);
                PageSize(rawLog.MetaData(), filePageSize);
                IsConnected(rawLog.MetaData(), true);

                using (LogBuffers logBuffers = new LogBuffers(rawLog.FileName()))
                {
                    Image image = new Image(null, 1, subscriberPosition, logBuffers, RethrowingErrorHandler.INSTANCE,
                        "127.0.0.1:123", 0);

                    subscriberPosition.Set(subscriberPositionThatWillTriggerException);

                    image.BoundedControlledPoll(
                        (buffer, offset, length, header) => ControlledFragmentHandlerAction.COMMIT, 1024, 1);
                    image.BoundedPoll((buffer, offset, length, header) => { }, 1024, 1);
                }
            }
        }
    }
    */
}