/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Adaptive.Aeron;
using Adaptive.Agrona.Concurrent;
using AeronClient = Adaptive.Aeron.Aeron;

namespace Adaptive.Archiver.IntegrationTests.Infrastructure
{
    /// <summary>
    /// Publishes binary control commands to the LossGenControlAgent running in the driver
    /// JVM. Opcodes and payload layout must mirror
    /// io.adaptive.aeron.test.lossgen.LossGenControlAgent.
    /// </summary>
    internal sealed class LossGenController : IDisposable
    {
        private const string ControlChannel = "aeron:ipc?session-id=99999";
        private const int ControlStreamId = 10001;

        private const byte CmdFrameDataEnable = 0x01;
        private const byte CmdFrameDataDisable = 0x02;
        private const byte CmdStreamIdEnable = 0x03;
        private const byte CmdStreamIdDisable = 0x04;
        private const byte CmdStreamIdFrameDataEnable = 0x05;
        private const byte CmdStreamIdFrameDataDisable = 0x06;
        private const byte CmdDataInRangeSetTarget = 0x07;
        private const byte CmdDataInRangeEnable = 0x08;
        private const byte CmdDataInRangeDisable = 0x09;
        private const byte CmdSetupAtPositionSetTarget = 0x0A;
        private const byte CmdSetupAtPositionEnable = 0x0B;
        private const byte CmdSetupAtPositionDisable = 0x0C;

        private const byte PredAlwaysTrue = 0x00;
        private const byte PredRandomFraction = 0x01;
        private const byte PredPayloadEqualsSticky = 0x02;

        private readonly Publication _publication;
        // 4KB scratch: enough for sticky-match payloads up to ~4KB (current tests use 1376).
        private readonly UnsafeBuffer _scratch = new UnsafeBuffer(new byte[4096]);

        public LossGenController(AeronClient aeron)
        {
            _publication = aeron.AddPublication(ControlChannel, ControlStreamId);
            Tests.Await(() => _publication.IsConnected);
        }

        public void EnableFrameDataAlwaysDrop()
        {
            _scratch.PutByte(0, CmdFrameDataEnable);
            _scratch.PutByte(1, PredAlwaysTrue);
            Offer(2);
        }

        public void EnableFrameDataRandom(double fraction)
        {
            _scratch.PutByte(0, CmdFrameDataEnable);
            _scratch.PutByte(1, PredRandomFraction);
            _scratch.PutDouble(2, fraction);
            Offer(10);
        }

        /// <summary>
        /// Enables the frame-data loss generator with a sticky-match predicate: drops everything
        /// from the first frame whose payload matches the target onward.
        /// </summary>
        public void EnableFrameDataPayloadSticky(byte[] match)
        {
            // 1 cmd + 1 pred + 4 length + match.Length bytes
            var required = 6 + match.Length;
            if (_scratch.Capacity < required)
            {
                throw new InvalidOperationException(
                    "sticky-match payload too large; bump scratch buffer");
            }
            _scratch.PutByte(0, CmdFrameDataEnable);
            _scratch.PutByte(1, PredPayloadEqualsSticky);
            _scratch.PutInt(2, match.Length);
            _scratch.PutBytes(6, match);
            Offer(required);
        }

        public void DisableFrameData()
        {
            _scratch.PutByte(0, CmdFrameDataDisable);
            Offer(1);
        }

        public void EnableStreamId(int streamId)
        {
            _scratch.PutByte(0, CmdStreamIdEnable);
            _scratch.PutInt(1, streamId);
            Offer(5);
        }

        public void DisableStreamId()
        {
            _scratch.PutByte(0, CmdStreamIdDisable);
            Offer(1);
        }

        public void EnableStreamIdFrameDataRandom(int streamId, double fraction)
        {
            _scratch.PutByte(0, CmdStreamIdFrameDataEnable);
            _scratch.PutInt(1, streamId);
            _scratch.PutByte(5, PredRandomFraction);
            _scratch.PutDouble(6, fraction);
            Offer(14);
        }

        /// <summary>
        /// Enables the stream-id frame-data loss generator with a sticky-match predicate:
        /// it drops everything from the first frame whose payload matches the target onward.
        /// </summary>
        public void EnableStreamIdFrameDataPayloadSticky(int streamId, byte[] match)
        {
            // 1 cmd + 4 streamId + 1 pred + 4 length + match.Length bytes
            var required = 10 + match.Length;
            if (_scratch.Capacity < required)
            {
                throw new InvalidOperationException(
                    "sticky-match payload too large; bump scratch buffer");
            }
            _scratch.PutByte(0, CmdStreamIdFrameDataEnable);
            _scratch.PutInt(1, streamId);
            _scratch.PutByte(5, PredPayloadEqualsSticky);
            _scratch.PutInt(6, match.Length);
            _scratch.PutBytes(10, match);
            Offer(required);
        }

        public void DisableStreamIdFrameData()
        {
            _scratch.PutByte(0, CmdStreamIdFrameDataDisable);
            Offer(1);
        }

        public void SetDataInRangeTarget(int streamId, int activeTermId, int min, int max)
        {
            _scratch.PutByte(0, CmdDataInRangeSetTarget);
            _scratch.PutInt(1, streamId);
            _scratch.PutInt(5, activeTermId);
            _scratch.PutInt(9, min);
            _scratch.PutInt(13, max);
            Offer(17);
        }

        public void EnableDataInRange()
        {
            _scratch.PutByte(0, CmdDataInRangeEnable);
            Offer(1);
        }

        public void DisableDataInRange()
        {
            _scratch.PutByte(0, CmdDataInRangeDisable);
            Offer(1);
        }

        public void SetSetupAtPositionTarget(int streamId, int initialTermId, int activeTermId, int termOffset)
        {
            _scratch.PutByte(0, CmdSetupAtPositionSetTarget);
            _scratch.PutInt(1, streamId);
            _scratch.PutInt(5, initialTermId);
            _scratch.PutInt(9, activeTermId);
            _scratch.PutInt(13, termOffset);
            Offer(17);
        }

        public void EnableSetupAtPosition()
        {
            _scratch.PutByte(0, CmdSetupAtPositionEnable);
            Offer(1);
        }

        public void DisableSetupAtPosition()
        {
            _scratch.PutByte(0, CmdSetupAtPositionDisable);
            Offer(1);
        }

        private void Offer(int length)
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (_publication.Offer(_scratch, 0, length) < 0)
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new InvalidOperationException("LossGenController: offer timed out");
                }
                System.Threading.Thread.Sleep(1);
            }
            // The agent on the driver side polls on a 1ms cadence. Sleep briefly so the in-flight
            // command has propagated before the test takes its next action — without this, tests
            // that enable loss and immediately publish race against the IPC delivery window.
            System.Threading.Thread.Sleep(50);
        }

        public void Dispose()
        {
            _publication.Dispose();
        }
    }
}
