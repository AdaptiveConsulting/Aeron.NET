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

using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.LogBuffer.io.aeron.logbuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    [TestFixture]
    public class TermGapFillerTest
    {
        private bool InstanceFieldsInitialized;
        private const int INITIAL_TERM_ID = 11;
        private const int TERM_ID = 22;
        private const int SESSION_ID = 333;
        private const int STREAM_ID = 7;

        private UnsafeBuffer metaDataBuffer;
        private UnsafeBuffer termBuffer;
        private DataHeaderFlyweight dataFlyweight;

        [SetUp]
        public virtual void Setup()
        {
            metaDataBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);
            termBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_MIN_LENGTH]);
            dataFlyweight = new DataHeaderFlyweight(termBuffer);
            LogBufferDescriptor.InitialTermId(metaDataBuffer, INITIAL_TERM_ID);
            LogBufferDescriptor.StoreDefaultFrameHeader(metaDataBuffer, DataHeaderFlyweight.CreateDefaultHeader(SESSION_ID, STREAM_ID, INITIAL_TERM_ID));
        }

        [Test]
        public virtual void ShouldFillGapAtBeginningOfTerm()
        {
            const int gapOffset = 0;
            const int gapLength = 64;

            Assert.IsTrue(TermGapFiller.TryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

            Assert.That(dataFlyweight.FrameLength(), Is.EqualTo(gapLength));
            Assert.That(dataFlyweight.TermOffset(), Is.EqualTo(gapOffset));
            Assert.That(dataFlyweight.SessionId(), Is.EqualTo(SESSION_ID));
            Assert.That(dataFlyweight.TermId(), Is.EqualTo(TERM_ID));
            Assert.That(dataFlyweight.HeaderType(), Is.EqualTo(FrameDescriptor.PADDING_FRAME_TYPE));
            Assert.That(dataFlyweight.Flags(), Is.EqualTo(FrameDescriptor.UNFRAGMENTED));
        }

        [Test]
        public virtual void ShouldNotOverwriteExistingFrame()
        {
            const int gapOffset = 0;
            const int gapLength = 64;

            dataFlyweight.FrameLength(32);

            Assert.IsFalse(TermGapFiller.TryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));
        }

        [Test]
        public virtual void ShouldFillGapAfterExistingFrame()
        {
            const int gapOffset = 128;
            const int gapLength = 64;

            dataFlyweight.SessionId(SESSION_ID).TermId(TERM_ID).StreamId(STREAM_ID).Flags(FrameDescriptor.UNFRAGMENTED).FrameLength(gapOffset);
            dataFlyweight.SetMemory(0, gapOffset - DataHeaderFlyweight.HEADER_LENGTH, (byte)'x');

            Assert.IsTrue(TermGapFiller.TryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

            dataFlyweight.Wrap(termBuffer, gapOffset, termBuffer.Capacity - gapOffset);
            Assert.That(dataFlyweight.FrameLength(), Is.EqualTo(gapLength));
            Assert.That(dataFlyweight.TermOffset(), Is.EqualTo(gapOffset));
            Assert.That(dataFlyweight.SessionId(), Is.EqualTo(SESSION_ID));
            Assert.That(dataFlyweight.TermId(), Is.EqualTo(TERM_ID));
            Assert.That(dataFlyweight.HeaderType(), Is.EqualTo(FrameDescriptor.PADDING_FRAME_TYPE));
            Assert.That(dataFlyweight.Flags(), Is.EqualTo(FrameDescriptor.UNFRAGMENTED));
        }

        [Test]
        public virtual void ShouldFillGapBetweenExistingFrames()
        {
            const int gapOffset = 128;
            const int gapLength = 64;

            dataFlyweight.SessionId(SESSION_ID).TermId(TERM_ID).TermOffset(0).StreamId(STREAM_ID).Flags(FrameDescriptor.UNFRAGMENTED).FrameLength(gapOffset).SetMemory(0, gapOffset - DataHeaderFlyweight.HEADER_LENGTH, (byte)'x');

            int secondExistingFrameOffset = gapOffset + gapLength;
            dataFlyweight.Wrap(termBuffer, secondExistingFrameOffset, termBuffer.Capacity - secondExistingFrameOffset);
            dataFlyweight.SessionId(SESSION_ID).TermId(TERM_ID).TermOffset(secondExistingFrameOffset).StreamId(STREAM_ID).Flags(FrameDescriptor.UNFRAGMENTED).FrameLength(64);

            Assert.IsTrue(TermGapFiller.TryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

            dataFlyweight.Wrap(termBuffer, gapOffset, termBuffer.Capacity - gapOffset);
            Assert.That(dataFlyweight.FrameLength(), Is.EqualTo(gapLength));
            Assert.That(dataFlyweight.TermOffset(), Is.EqualTo(gapOffset));
            Assert.That(dataFlyweight.SessionId(), Is.EqualTo(SESSION_ID));
            Assert.That(dataFlyweight.TermId(), Is.EqualTo(TERM_ID));
            Assert.That(dataFlyweight.HeaderType(), Is.EqualTo(FrameDescriptor.PADDING_FRAME_TYPE));
            Assert.That(dataFlyweight.Flags(), Is.EqualTo(FrameDescriptor.UNFRAGMENTED));
        }

        [Test]
        public virtual void ShouldFillGapAtEndOfTerm()
        {
            int gapOffset = termBuffer.Capacity - 64;
            const int gapLength = 64;

            dataFlyweight.SessionId(SESSION_ID).TermId(TERM_ID).StreamId(STREAM_ID).Flags(FrameDescriptor.UNFRAGMENTED).FrameLength(termBuffer.Capacity - gapOffset);
            dataFlyweight.SetMemory(0, gapOffset - DataHeaderFlyweight.HEADER_LENGTH, (byte)'x');

            Assert.IsTrue(TermGapFiller.TryFillGap(metaDataBuffer, termBuffer, TERM_ID, gapOffset, gapLength));

            dataFlyweight.Wrap(termBuffer, gapOffset, termBuffer.Capacity - gapOffset);
            Assert.That(dataFlyweight.FrameLength(), Is.EqualTo(gapLength));
            Assert.That(dataFlyweight.TermOffset(), Is.EqualTo(gapOffset));
            Assert.That(dataFlyweight.SessionId(), Is.EqualTo(SESSION_ID));
            Assert.That(dataFlyweight.TermId(), Is.EqualTo(TERM_ID));
            Assert.That(dataFlyweight.HeaderType(), Is.EqualTo(FrameDescriptor.PADDING_FRAME_TYPE));
            Assert.That((byte)dataFlyweight.Flags(), Is.EqualTo(FrameDescriptor.UNFRAGMENTED));
        }
    }
}
