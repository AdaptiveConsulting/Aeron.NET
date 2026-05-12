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
    public class ImageTest
    {
        private const int TermBufferLength = LogBufferDescriptor.TERM_MIN_LENGTH;

        private static readonly int PositionBitsToShift = LogBufferDescriptor.PositionBitsToShift(TermBufferLength);

        private static readonly byte[] Data = new byte[36];

        static ImageTest()
        {
            for (var i = 0; i < Data.Length; i++)
            {
                Data[i] = (byte)i;
            }
        }

        private const long CorrelationId = 0xC044E1AL;
        private const int SessionId = 0x5E55101D;
        private const int StreamId = 0xC400E;
        private const string SourceIdentity = "ipc";
        private const int InitialTermId = 0xEE81D;
        private static readonly int MessageLength = DataHeaderFlyweight.HEADER_LENGTH + Data.Length;

        private static readonly int AlignedFrameLength = BitUtil.Align(MessageLength, FrameDescriptor.FRAME_ALIGNMENT);

        private UnsafeBuffer _rcvBuffer;
        private DataHeaderFlyweight _dataHeader;
        private IFragmentHandler _mockFragmentHandler;
        private IControlledFragmentHandler _mockControlledFragmentHandler;
        private IPosition _position;
        private LogBuffers _logBuffers;
        private IErrorHandler _errorHandler;
        private Subscription _subscription;

        private UnsafeBuffer[] _termBuffers;

        [SetUp]
        public void SetUp()
        {
            _rcvBuffer = new UnsafeBuffer(new byte[AlignedFrameLength]);
            _dataHeader = new DataHeaderFlyweight();
            _mockFragmentHandler = A.Fake<IFragmentHandler>();
            _mockControlledFragmentHandler = A.Fake<IControlledFragmentHandler>();
            _position = A.Fake<IPosition>(options => options.Wrapping(new AtomicLongPosition()));
            _logBuffers = A.Fake<LogBuffers>();
            _errorHandler = A.Fake<IErrorHandler>();
            _subscription = A.Fake<Subscription>();

            _termBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];

            _dataHeader.Wrap(_rcvBuffer);

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                _termBuffers[i] = new UnsafeBuffer(new byte[TermBufferLength]);
            }

            var logMetaDataBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);

            A.CallTo(() => _logBuffers.DuplicateTermBuffers()).Returns(_termBuffers);
            A.CallTo(() => _logBuffers.TermLength()).Returns(TermBufferLength);
            A.CallTo(() => _logBuffers.MetaDataBuffer()).Returns(logMetaDataBuffer);
        }

        [Test]
        public void ShouldHandleClosedImage()
        {
            var image = CreateImage();

            image.Close();

            Assert.True(image.Closed);
            Assert.AreEqual(0, image.Poll(_mockFragmentHandler, int.MaxValue));
            Assert.AreEqual(0, image.Position);
        }

        [Test]
        public void ShouldAllowValidPosition()
        {
            var image = CreateImage();
            var expectedPosition = TermBufferLength - 32;

            _position.SetRelease(expectedPosition);
            Assert.AreEqual(image.Position, expectedPosition);

            image.Position = TermBufferLength;
            Assert.AreEqual(image.Position, (long)TermBufferLength);
        }

        [Test]
        public void ShouldNotAdvancePastEndOfTerm()
        {
            var image = CreateImage();
            var expectedPosition = TermBufferLength - 32;

            _position.SetRelease(expectedPosition);
            Assert.AreEqual(image.Position, expectedPosition);

            Assert.Throws<ArgumentException>(() => image.Position = TermBufferLength + 32);
        }

        [Test]
        public void ShouldReportCorrectPositionOnReception()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));

            var messages = image.Poll(_mockFragmentHandler, int.MaxValue);
            Assert.AreEqual(1, messages);

            A.CallTo(() =>
                    _mockFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened();

            A.CallTo(() => _position.SetRelease(initialPosition))
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId()
        {
            const int initialMessageIndex = 5;
            var initialTermOffset = OffsetForFrame(initialMessageIndex);
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                initialTermOffset,
                PositionBitsToShift,
                InitialTermId
            );

            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(initialMessageIndex));

            var messages = image.Poll(_mockFragmentHandler, int.MaxValue);
            Assert.AreEqual(1, messages);

            A.CallTo(() =>
                    _mockFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        initialTermOffset + DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened();

            A.CallTo(() => _position.SetRelease(initialPosition))
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId()
        {
            var activeTermId = InitialTermId + 1;
            const int initialMessageIndex = 5;
            var initialTermOffset = OffsetForFrame(initialMessageIndex);
            var initialPosition = LogBufferDescriptor.ComputePosition(
                activeTermId,
                initialTermOffset,
                PositionBitsToShift,
                InitialTermId
            );

            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(activeTermId, OffsetForFrame(initialMessageIndex));

            var messages = image.Poll(_mockFragmentHandler, int.MaxValue);
            Assert.AreEqual(1, messages);

            A.CallTo(() =>
                    _mockFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        initialTermOffset + DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened();

            A.CallTo(() => _position.SetRelease(initialPosition))
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldPollNoFragmentsToControlledFragmentHandler()
        {
            var image = CreateImage();
            var fragmentsRead = image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(0, fragmentsRead);

            A.CallTo(() => _position.SetRelease(A<long>._)).MustNotHaveHappened();
            A.CallTo(() => _mockFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._))
                .MustNotHaveHappened();
        }

        [Test]
        public void ShouldPollOneFragmentToControlledFragmentHandlerOnContinue()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(1, fragmentsRead);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldUpdatePositionOnRethrownExceptionInControlledPoll()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Throws(new Exception());

            A.CallTo(_errorHandler).Throws(new Exception());

            bool thrown = false;

            try
            {
                image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);
            }
            catch (Exception)
            {
                thrown = true;
            }

            Assert.True(thrown);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened();
        }

        [Test]
        public void ShouldUpdatePositionOnRethrownExceptionInPoll()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));

            A.CallTo(() => _mockFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._))
                .Throws(new Exception());

            A.CallTo(_errorHandler).Throws(new Exception());

            bool thrown = false;

            try
            {
                image.Poll(_mockFragmentHandler, int.MaxValue);
            }
            catch (Exception)
            {
                thrown = true;
            }

            Assert.True(thrown);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);

            A.CallTo(() =>
                    _mockFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened();
        }

        [Test]
        public void ShouldNotPollOneFragmentToControlledFragmentHandlerOnAbort()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.ABORT);

            var fragmentsRead = image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(0, fragmentsRead);
            Assert.AreEqual(initialPosition, image.Position);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened();
        }

        [Test]
        public void ShouldPollOneFragmentToControlledFragmentHandlerOnBreak()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.BREAK);

            var fragmentsRead = image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(1, fragmentsRead);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldPollFragmentsToControlledFragmentHandlerOnCommit()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.COMMIT);

            var fragmentsRead = image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(2, fragmentsRead);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened())
                .Then(
                    A.CallTo(() =>
                            _mockControlledFragmentHandler.OnFragment(
                                A<UnsafeBuffer>._,
                                AlignedFrameLength + DataHeaderFlyweight.HEADER_LENGTH,
                                Data.Length,
                                A<Header>._
                            )
                        )
                        .MustHaveHappened()
                )
                .Then(
                    A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength * 2L)).MustHaveHappened()
                );
        }

        [Test]
        public void ShouldPollNoFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeInitialPosition()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            var maxPosition = initialPosition - DataHeaderFlyweight.HEADER_LENGTH;
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.BoundedControlledPoll(_mockControlledFragmentHandler, maxPosition, int.MaxValue);

            Assert.That(fragmentsRead, Is.EqualTo(0));
            Assert.That(_position.Get(), Is.EqualTo(initialPosition));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustNotHaveHappened();
        }

        [Test]
        public void ShouldPollFragmentsToBoundedControlledFragmentHandlerWithInitialOffsetNotZero()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                OffsetForFrame(1),
                PositionBitsToShift,
                InitialTermId
            );
            var maxPosition = initialPosition + AlignedFrameLength;
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(1));
            InsertDataFrame(InitialTermId, OffsetForFrame(2));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.BoundedControlledPoll(_mockControlledFragmentHandler, maxPosition, int.MaxValue);

            Assert.That(fragmentsRead, Is.EqualTo(1));
            Assert.That(_position.Get(), Is.EqualTo(maxPosition));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .MustHaveHappened();
        }

        [Test]
        public void ShouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionBeforeNextMessage()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            var maxPosition = initialPosition + AlignedFrameLength;
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.BoundedControlledPoll(_mockControlledFragmentHandler, maxPosition, int.MaxValue);

            Assert.That(fragmentsRead, Is.EqualTo(1));
            Assert.That(_position.Get(), Is.EqualTo(maxPosition));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldPollFragmentsToBoundedFragmentHandlerWithMaxPositionBeforeNextMessage()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            var maxPosition = initialPosition + AlignedFrameLength;
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));

            var fragmentsRead = image.BoundedPoll(_mockFragmentHandler, maxPosition, int.MaxValue);

            Assert.That(fragmentsRead, Is.EqualTo(1));

            A.CallTo(() =>
                    _mockFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldPollFragmentsToBoundedControlledFragmentHandlerWithMaxPositionAfterEndOfTerm()
        {
            var initialOffset = TermBufferLength - (AlignedFrameLength * 2);
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                initialOffset,
                PositionBitsToShift,
                InitialTermId
            );
            var maxPosition = initialPosition + TermBufferLength;
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, initialOffset);
            InsertPaddingFrame(InitialTermId, initialOffset + AlignedFrameLength);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.BoundedControlledPoll(_mockControlledFragmentHandler, maxPosition, int.MaxValue);
            Assert.That(fragmentsRead, Is.EqualTo(1));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        initialOffset + DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(TermBufferLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldUpdatePositionToEndOfCommittedFragmentOnCommit()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));
            InsertDataFrame(InitialTermId, OffsetForFrame(2));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .ReturnsNextFromSequence(
                    ControlledFragmentHandlerAction.CONTINUE,
                    ControlledFragmentHandlerAction.COMMIT,
                    ControlledFragmentHandlerAction.CONTINUE
                );

            var fragmentsRead = image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(3, fragmentsRead);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(
                    A.CallTo(() =>
                            _mockControlledFragmentHandler.OnFragment(
                                A<UnsafeBuffer>._,
                                AlignedFrameLength + DataHeaderFlyweight.HEADER_LENGTH,
                                Data.Length,
                                A<Header>._
                            )
                        )
                        .MustHaveHappened()
                )
                .Then(
                    A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength * 2L)).MustHaveHappened()
                )
                .Then(
                    A.CallTo(() =>
                            _mockControlledFragmentHandler.OnFragment(
                                A<UnsafeBuffer>._,
                                2 * AlignedFrameLength + DataHeaderFlyweight.HEADER_LENGTH,
                                Data.Length,
                                A<Header>._
                            )
                        )
                        .MustHaveHappened()
                )
                .Then(
                    A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength * 3L)).MustHaveHappened()
                );
        }

        [Test]
        public void ShouldPollFragmentsToControlledFragmentHandlerOnContinue()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)
                )
                .Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.ControlledPoll(_mockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(2, fragmentsRead);

            A.CallTo(() =>
                    _mockControlledFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(
                    A.CallTo(() =>
                            _mockControlledFragmentHandler.OnFragment(
                                A<UnsafeBuffer>._,
                                AlignedFrameLength + DataHeaderFlyweight.HEADER_LENGTH,
                                Data.Length,
                                A<Header>._
                            )
                        )
                        .MustHaveHappened()
                )
                .Then(
                    A.CallTo(() => _position.SetRelease(initialPosition + AlignedFrameLength * 2L)).MustHaveHappened()
                );
        }

        [Test]
        public void ShouldPollFragmentsToBoundedFragmentHandlerWithMaxPositionAboveIntMaxValue()
        {
            var initialOffset = TermBufferLength - (AlignedFrameLength * 2);
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                initialOffset,
                PositionBitsToShift,
                InitialTermId
            );
            var maxPosition = (long)int.MaxValue + 1000;
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, initialOffset);
            InsertPaddingFrame(InitialTermId, initialOffset + AlignedFrameLength);

            var fragmentsRead = image.BoundedPoll(_mockFragmentHandler, maxPosition, int.MaxValue);
            Assert.That(fragmentsRead, Is.EqualTo(1));

            A.CallTo(() =>
                    _mockFragmentHandler.OnFragment(
                        A<UnsafeBuffer>._,
                        initialOffset + DataHeaderFlyweight.HEADER_LENGTH,
                        Data.Length,
                        A<Header>._
                    )
                )
                .MustHaveHappened()
                .Then(A.CallTo(() => _position.SetRelease(TermBufferLength)).MustHaveHappened());
        }

        [Test]
        public void BlockPollDeliversBlockAndAdvancesPosition()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));

            int handlerCallCount = 0;
            int receivedOffset = -1,
                receivedLength = -1,
                receivedSessionId = -1,
                receivedTermId = -1;
            var bytes = image.BlockPoll(
                (buffer, offset, length, sessionId, termId) =>
                {
                    handlerCallCount++;
                    receivedOffset = offset;
                    receivedLength = length;
                    receivedSessionId = sessionId;
                    receivedTermId = termId;
                },
                int.MaxValue
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            Assert.AreEqual(1, handlerCallCount);
            Assert.AreEqual(0, receivedOffset);
            Assert.AreEqual(AlignedFrameLength, receivedLength);
            Assert.AreEqual(SessionId, receivedSessionId);
            Assert.AreEqual(InitialTermId, receivedTermId);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);
        }

        [Test]
        public void BlockPollReturnsZeroAndDoesNothingWhenClosed()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            image.Close();

            bool handlerCalled = false;
            var bytes = image.BlockPoll((b, o, l, s, t) => handlerCalled = true, int.MaxValue);

            Assert.AreEqual(0, bytes);
            Assert.IsFalse(handlerCalled);
            Assert.AreEqual(initialPosition, image.Position);
        }

        [Test]
        public void BlockPollReturnsZeroWhenNoFramesAvailable()
        {
            var image = CreateImage();

            bool handlerCalled = false;
            var bytes = image.BlockPoll((b, o, l, s, t) => handlerCalled = true, int.MaxValue);

            Assert.AreEqual(0, bytes);
            Assert.IsFalse(handlerCalled);
        }

        [Test]
        public void BlockPollRespectsBlockLengthLimit()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            InsertDataFrame(InitialTermId, OffsetForFrame(1));

            int handlerCallCount = 0;
            int receivedLength = -1;
            var bytes = image.BlockPoll(
                (b, o, l, s, t) =>
                {
                    handlerCallCount++;
                    receivedLength = l;
                },
                AlignedFrameLength + 1
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            Assert.AreEqual(1, handlerCallCount);
            Assert.AreEqual(AlignedFrameLength, receivedLength);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);
        }

        [Test]
        public void BlockPollDeliversPaddingFrameWhenFirstFrameIsPadding()
        {
            // Padding frame placed near end of term so the helper's TermRebuilder.Insert call
            // copies only ALIGNED_FRAME_LENGTH bytes (RcvBuffer's capacity). Scan starts at
            // paddingOffset and the first frame is padding, exercising the corner case.
            int paddingOffset = TermBufferLength - AlignedFrameLength;
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                paddingOffset,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertPaddingFrame(InitialTermId, paddingOffset);

            int handlerCallCount = 0;
            int receivedLength = -1;
            var bytes = image.BlockPoll(
                (b, o, l, s, t) =>
                {
                    handlerCallCount++;
                    receivedLength = l;
                },
                TermBufferLength
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            Assert.AreEqual(1, handlerCallCount);
            Assert.AreEqual(AlignedFrameLength, receivedLength);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);
        }

        [Test]
        public void RawPollDeliversBlockAndAdvancesPosition()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));

            int handlerCallCount = 0;
            long receivedFileOffset = -1;
            int receivedTermOffset = -1,
                receivedLength = -1,
                receivedSessionId = -1,
                receivedTermId = -1;
            var bytes = image.RawPoll(
                (fs, fileOffset, buf, termOffset, length, sessionId, termId) =>
                {
                    handlerCallCount++;
                    receivedFileOffset = fileOffset;
                    receivedTermOffset = termOffset;
                    receivedLength = length;
                    receivedSessionId = sessionId;
                    receivedTermId = termId;
                },
                int.MaxValue
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            Assert.AreEqual(1, handlerCallCount);
            Assert.AreEqual(0L, receivedFileOffset);
            Assert.AreEqual(0, receivedTermOffset);
            Assert.AreEqual(AlignedFrameLength, receivedLength);
            Assert.AreEqual(SessionId, receivedSessionId);
            Assert.AreEqual(InitialTermId, receivedTermId);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);
        }

        [Test]
        public void RawPollComputesFileOffsetForNonZeroPartition()
        {
            // Place the subscriber position into partition index 2; fileOffset must be
            // (capacity * activeIndex) + termOffset.
            int activeTermId = InitialTermId + 2;
            int termOffset = OffsetForFrame(0);
            var initialPosition = LogBufferDescriptor.ComputePosition(
                activeTermId,
                termOffset,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(activeTermId, termOffset);

            long receivedFileOffset = -1;
            var bytes = image.RawPoll(
                (fs, fileOffset, buf, to, l, s, t) => receivedFileOffset = fileOffset,
                int.MaxValue
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            long expectedFileOffset = (long)TermBufferLength * 2 + termOffset;
            Assert.AreEqual(expectedFileOffset, receivedFileOffset);
        }

        [Test]
        public void RawPollReturnsZeroAndDoesNothingWhenClosed()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                0,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, OffsetForFrame(0));
            image.Close();

            bool handlerCalled = false;
            var bytes = image.RawPoll((fs, fo, buf, to, l, s, t) => handlerCalled = true, int.MaxValue);

            Assert.AreEqual(0, bytes);
            Assert.IsFalse(handlerCalled);
            Assert.AreEqual(initialPosition, image.Position);
        }

        [Test]
        public void RawPollReturnsZeroWhenNoFramesAvailable()
        {
            var image = CreateImage();

            bool handlerCalled = false;
            var bytes = image.RawPoll((fs, fo, buf, to, l, s, t) => handlerCalled = true, int.MaxValue);

            Assert.AreEqual(0, bytes);
            Assert.IsFalse(handlerCalled);
        }

        [Test]
        public void RawPollDeliversPaddingFrameWhenFirstFrameIsPadding()
        {
            int paddingOffset = TermBufferLength - AlignedFrameLength;
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                paddingOffset,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertPaddingFrame(InitialTermId, paddingOffset);

            int handlerCallCount = 0;
            int receivedLength = -1;
            var bytes = image.RawPoll(
                (fs, fo, buf, to, l, s, t) =>
                {
                    handlerCallCount++;
                    receivedLength = l;
                },
                TermBufferLength
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            Assert.AreEqual(1, handlerCallCount);
            Assert.AreEqual(AlignedFrameLength, receivedLength);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);
        }

        [Test]
        public void BlockPollClampsBlockLengthLimitWithoutIntegerOverflow()
        {
            int frameOffset = AlignedFrameLength;
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                frameOffset,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, frameOffset);

            int handlerCallCount = 0;
            int receivedLength = -1;
            var bytes = image.BlockPoll(
                (b, o, l, s, t) =>
                {
                    handlerCallCount++;
                    receivedLength = l;
                },
                int.MaxValue
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            Assert.AreEqual(1, handlerCallCount);
            Assert.AreEqual(AlignedFrameLength, receivedLength);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);
        }

        [Test]
        public void RawPollClampsBlockLengthLimitWithoutIntegerOverflow()
        {
            int frameOffset = AlignedFrameLength;
            var initialPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                frameOffset,
                PositionBitsToShift,
                InitialTermId
            );
            _position.SetRelease(initialPosition);
            var image = CreateImage();

            InsertDataFrame(InitialTermId, frameOffset);

            int handlerCallCount = 0;
            int receivedLength = -1;
            var bytes = image.RawPoll(
                (fs, fo, buf, to, l, s, t) =>
                {
                    handlerCallCount++;
                    receivedLength = l;
                },
                int.MaxValue
            );

            Assert.AreEqual(AlignedFrameLength, bytes);
            Assert.AreEqual(1, handlerCallCount);
            Assert.AreEqual(AlignedFrameLength, receivedLength);
            Assert.AreEqual(initialPosition + AlignedFrameLength, image.Position);
        }

        private Image CreateImage()
        {
            return new Image(
                _subscription,
                SessionId,
                _position,
                _logBuffers,
                _errorHandler,
                SourceIdentity,
                CorrelationId
            );
        }

        private void InsertDataFrame(int activeTermId, int termOffset)
        {
            _dataHeader
                .TermId(InitialTermId)
                .StreamId(StreamId)
                .SessionId(SessionId)
                .TermOffset(termOffset)
                .FrameLength(Data.Length + DataHeaderFlyweight.HEADER_LENGTH)
                .HeaderType(HeaderFlyweight.HDR_TYPE_DATA)
                .Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                .Version(HeaderFlyweight.CURRENT_VERSION);

            _rcvBuffer.PutBytes(_dataHeader.DataOffset(), Data);

            var activeIndex = LogBufferDescriptor.IndexByTerm(InitialTermId, activeTermId);
            TermRebuilder.Insert(_termBuffers[activeIndex], termOffset, _rcvBuffer, AlignedFrameLength);
        }

        private void InsertPaddingFrame(int activeTermId, int termOffset)
        {
            _dataHeader
                .TermId(InitialTermId)
                .StreamId(StreamId)
                .SessionId(SessionId)
                .FrameLength(TermBufferLength - termOffset)
                .HeaderType(HeaderFlyweight.HDR_TYPE_PAD)
                .Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                .Version(HeaderFlyweight.CURRENT_VERSION);

            var activeIndex = LogBufferDescriptor.IndexByTerm(InitialTermId, activeTermId);
            TermRebuilder.Insert(_termBuffers[activeIndex], termOffset, _rcvBuffer, TermBufferLength - termOffset);
        }

        private static int OffsetForFrame(int index)
        {
            return index * AlignedFrameLength;
        }
    }
}
