/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Description of the command and control file used between driver and clients
    /// 
    /// File Layout
    /// <pre>
    ///  +----------------------------+
    ///  |      Aeron CnC Version     |
    ///  +----------------------------+
    ///  |          Meta Data         |
    ///  +----------------------------+
    ///  |      to-driver Buffer      |
    ///  +----------------------------+
    ///  |      to-clients Buffer     |
    ///  +----------------------------+
    ///  |  Counters Metadata Buffer  |
    ///  +----------------------------+
    ///  |   Counters Values Buffer   |
    ///  +----------------------------+
    ///  |          Error Log         |
    ///  +----------------------------+
    /// </pre>
    /// 
    /// Meta Data Layout <see cref="CNC_VERSION"/>
    /// <pre>
    ///  0                   1                   2                   3
    ///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                      Aeron CnC Version                        |
    /// +---------------------------------------------------------------+
    /// |                   to-driver buffer length                     |
    /// +---------------------------------------------------------------+
    /// |                  to-clients buffer length                     |
    /// +---------------------------------------------------------------+
    /// |               Counters Metadata buffer length                 |
    /// +---------------------------------------------------------------+
    /// |                Counters Values buffer length                  |
    /// +---------------------------------------------------------------+
    /// |                   Error Log buffer length                     |
    /// +---------------------------------------------------------------+
    /// |                   Client Liveness Timeout                     |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                    Driver Start Timestamp                     |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                         Driver PID                            |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public class CncFileDescriptor
    {
        public const string CNC_FILE = "cnc.dat";
        public const int CNC_VERSION = 15;

        public static readonly int CNC_VERSION_FIELD_OFFSET;
        public static readonly int TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
        public static readonly int ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int META_DATA_LENGTH;
        public static readonly int END_OF_METADATA_OFFSET;
        public static readonly int START_TIMESTAMP_FIELD_OFFSET;
        public static readonly int PID_FIELD_OFFSET;

        static CncFileDescriptor()
        {
            CNC_VERSION_FIELD_OFFSET = 0;
            TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET = CNC_VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET = TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET = TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET = COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET = COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET = ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            START_TIMESTAMP_FIELD_OFFSET = CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
            PID_FIELD_OFFSET = START_TIMESTAMP_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;

            META_DATA_LENGTH = PID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
            END_OF_METADATA_OFFSET = BitUtil.Align(META_DATA_LENGTH, BitUtil.CACHE_LINE_LENGTH * 2);
        }

        /// <summary>
        /// Compute the length of the cnc file and return it.
        /// </summary>
        /// <param name="totalLengthOfBuffers"> in bytes </param>
        /// <param name="alignment"> for file length to adhere to</param>
        /// <returns> cnc file length in bytes </returns>
        public static int ComputeCncFileLength(int totalLengthOfBuffers, int alignment)
        {
            return BitUtil.Align(END_OF_METADATA_OFFSET + totalLengthOfBuffers, alignment);
        }

        public static int CncVersionOffset(int baseOffset)
        {
            return baseOffset + CNC_VERSION_FIELD_OFFSET;
        }

        public static int ToDriverBufferLengthOffset(int baseOffset)
        {
            return baseOffset + TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int ToClientsBufferLengthOffset(int baseOffset)
        {
            return baseOffset + TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int CountersMetaDataBufferLengthOffset(int baseOffset)
        {
            return baseOffset + COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int CountersValuesBufferLengthOffset(int baseOffset)
        {
            return baseOffset + COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int ClientLivenessTimeoutOffset(int baseOffset)
        {
            return baseOffset + CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
        }

        public static int ErrorLogBufferLengthOffset(int baseOffset)
        {
            return baseOffset + ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int StartTimestampOffset(int baseOffset)
        {
            return baseOffset + START_TIMESTAMP_FIELD_OFFSET;
        }

        public static int PidOffset(int baseOffset)
        {
            return baseOffset + PID_FIELD_OFFSET;
        }

        public static void FillMetaData(
            UnsafeBuffer cncMetaDataBuffer, 
            int toDriverBufferLength, 
            int toClientsBufferLength, 
            int counterMetaDataBufferLength, 
            int counterValuesBufferLength, 
            long clientLivenessTimeout, 
            int errorLogBufferLength,
            long startTimestamp,
            long pid)
        {
            cncMetaDataBuffer.PutInt(ToDriverBufferLengthOffset(0), toDriverBufferLength);
            cncMetaDataBuffer.PutInt(ToClientsBufferLengthOffset(0), toClientsBufferLength);
            cncMetaDataBuffer.PutInt(CountersMetaDataBufferLengthOffset(0), counterMetaDataBufferLength);
            cncMetaDataBuffer.PutInt(CountersValuesBufferLengthOffset(0), counterValuesBufferLength);
            cncMetaDataBuffer.PutInt(ErrorLogBufferLengthOffset(0), errorLogBufferLength);
            cncMetaDataBuffer.PutLong(ClientLivenessTimeoutOffset(0), clientLivenessTimeout);
            cncMetaDataBuffer.PutLong(StartTimestampOffset(0), startTimestamp);
            cncMetaDataBuffer.PutLong(PidOffset(0), pid);
        }

        public static void SignalCncReady(UnsafeBuffer cncMetaDataBuffer)
        {
            cncMetaDataBuffer.PutIntVolatile(CncVersionOffset(0), CNC_VERSION);
        }

        public static UnsafeBuffer CreateMetaDataBuffer(MappedByteBuffer buffer)
        {
            return new UnsafeBuffer(buffer.Pointer, 0, META_DATA_LENGTH);
        }

        public static UnsafeBuffer CreateToDriverBuffer(MappedByteBuffer buffer, IDirectBuffer metaDataBuffer)
        {
            return new UnsafeBuffer(buffer.Pointer, END_OF_METADATA_OFFSET, metaDataBuffer.GetInt(ToDriverBufferLengthOffset(0)));
        }

        public static UnsafeBuffer CreateToClientsBuffer(MappedByteBuffer buffer, IDirectBuffer metaDataBuffer)
        {
            var offset = END_OF_METADATA_OFFSET + metaDataBuffer.GetInt(ToDriverBufferLengthOffset(0));

            return new UnsafeBuffer(buffer.Pointer, offset, metaDataBuffer.GetInt(ToClientsBufferLengthOffset(0)));
        }

        public static UnsafeBuffer CreateCountersMetaDataBuffer(MappedByteBuffer buffer, IDirectBuffer metaDataBuffer)
        {
            var offset = END_OF_METADATA_OFFSET + metaDataBuffer.GetInt(ToDriverBufferLengthOffset(0)) + metaDataBuffer.GetInt(ToClientsBufferLengthOffset(0));

            return new UnsafeBuffer(buffer.Pointer, offset, metaDataBuffer.GetInt(CountersMetaDataBufferLengthOffset(0)));
        }

        public static UnsafeBuffer CreateCountersValuesBuffer(MappedByteBuffer buffer, IDirectBuffer metaDataBuffer)
        {
            var offset = END_OF_METADATA_OFFSET + metaDataBuffer.GetInt(ToDriverBufferLengthOffset(0)) + metaDataBuffer.GetInt(ToClientsBufferLengthOffset(0)) + metaDataBuffer.GetInt(CountersMetaDataBufferLengthOffset(0));

            return new UnsafeBuffer(buffer.Pointer, offset, metaDataBuffer.GetInt(CountersValuesBufferLengthOffset(0)));
        }

        public static UnsafeBuffer CreateErrorLogBuffer(MappedByteBuffer buffer, IDirectBuffer metaDataBuffer)
        {
            var offset = END_OF_METADATA_OFFSET + metaDataBuffer.GetInt(ToDriverBufferLengthOffset(0)) + metaDataBuffer.GetInt(ToClientsBufferLengthOffset(0)) + metaDataBuffer.GetInt(CountersMetaDataBufferLengthOffset(0)) + metaDataBuffer.GetInt(CountersValuesBufferLengthOffset(0));

            return new UnsafeBuffer(buffer.Pointer, offset, metaDataBuffer.GetInt(ErrorLogBufferLengthOffset(0)));
        }

        public static long ClientLivenessTimeout(IDirectBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetLong(ClientLivenessTimeoutOffset(0));
        }

        public static long StartTimestamp(IDirectBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetLong(StartTimestampOffset(0));
        }

        public static long Pid(IDirectBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetLong(PidOffset(0));
        }
    }
}