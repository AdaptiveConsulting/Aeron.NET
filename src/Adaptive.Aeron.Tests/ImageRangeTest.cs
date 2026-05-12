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
