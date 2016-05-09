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
    ///  |   Counter Metadata Buffer  |
    ///  +----------------------------+
    ///  |    Counter Values Buffer   |
    ///  +----------------------------+
    ///  |          Error Log         |
    ///  +----------------------------+
    /// </pre>
    /// 
    /// Meta Data Layout (CnC Version 4)
    /// <pre>
    ///  +----------------------------+
    ///  |   to-driver buffer length  |
    ///  +----------------------------+
    ///  |  to-clients buffer length  |
    ///  +----------------------------+
    ///  |   metadata buffer length   |
    ///  +----------------------------+
    ///  |    values buffer length    |
    ///  +----------------------------+
    ///  |   Client Liveness Timeout  |
    ///  |                            |
    ///  +----------------------------+
    ///  |      Error Log length      |
    ///  +----------------------------+
    /// </pre>
    /// </summary>
    public class CncFileDescriptor
    {
        public const string CNC_FILE = "cnc.dat";

        public const int CNC_VERSION = 4;

        public static readonly int CNC_VERSION_FIELD_OFFSET;
        public static readonly int CNC_METADATA_OFFSET;

        /* Meta Data Offsets (offsets within the meta data section) */

        public static readonly int TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
        public static readonly int CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
        public static readonly int ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;

        static CncFileDescriptor()
        {
            CNC_VERSION_FIELD_OFFSET = 0;
            CNC_METADATA_OFFSET = CNC_VERSION_FIELD_OFFSET + BitUtil.SIZE_OF_INT;

            TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET = 0;
            TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET = TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET = TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET = COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET = COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET = CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
            META_DATA_LENGTH = ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
            END_OF_METADATA_OFFSET = BitUtil.Align(BitUtil.SIZE_OF_INT + META_DATA_LENGTH, (BitUtil.CACHE_LINE_LENGTH*2));
        }

        public static readonly int META_DATA_LENGTH;

        public static readonly int END_OF_METADATA_OFFSET;

        /// <summary>
        /// Compute the length of the cnc file and return it.
        /// </summary>
        /// <param name="totalLengthOfBuffers"> in bytes </param>
        /// <returns> cnc file length in bytes </returns>
        public static int ComputeCncFileLength(int totalLengthOfBuffers)
        {
            return END_OF_METADATA_OFFSET + totalLengthOfBuffers;
        }

        public static int CncVersionOffset(int baseOffset)
        {
            return baseOffset + CNC_VERSION_FIELD_OFFSET;
        }

        public static int ToDriverBufferLengthOffset(int baseOffset)
        {
            return baseOffset + CNC_METADATA_OFFSET + TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int ToClientsBufferLengthOffset(int baseOffset)
        {
            return baseOffset + CNC_METADATA_OFFSET + TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int CountersMetaDataBufferLengthOffset(int baseOffset)
        {
            return baseOffset + CNC_METADATA_OFFSET + COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int CountersValuesBufferLengthOffset(int baseOffset)
        {
            return baseOffset + CNC_METADATA_OFFSET + COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static int ClientLivenessTimeoutOffset(int baseOffset)
        {
            return baseOffset + CNC_METADATA_OFFSET + CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET;
        }

        public static int ErrorLogBufferLengthOffset(int baseOffset)
        {
            return baseOffset + CNC_METADATA_OFFSET + ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET;
        }

        public static void FillMetaData(UnsafeBuffer cncMetaDataBuffer, int toDriverBufferLength, int toClientsBufferLength, int counterMetaDataBufferLength, int counterValuesBufferLength, long clientLivenessTimeout, int errorLogBufferLength)
        {
            cncMetaDataBuffer.PutInt(CncVersionOffset(0), CNC_VERSION);
            cncMetaDataBuffer.PutInt(ToDriverBufferLengthOffset(0), toDriverBufferLength);
            cncMetaDataBuffer.PutInt(ToClientsBufferLengthOffset(0), toClientsBufferLength);
            cncMetaDataBuffer.PutInt(CountersMetaDataBufferLengthOffset(0), counterMetaDataBufferLength);
            cncMetaDataBuffer.PutInt(CountersValuesBufferLengthOffset(0), counterValuesBufferLength);
            cncMetaDataBuffer.PutLong(ClientLivenessTimeoutOffset(0), clientLivenessTimeout);
            cncMetaDataBuffer.PutInt(ErrorLogBufferLengthOffset(0), errorLogBufferLength);
        }

        // TODO find the best replacement for ByteBuffer..

        public static UnsafeBuffer CreateMetaDataBuffer(MappedByteBuffer buffer)
        {
            return new UnsafeBuffer(buffer.Pointer, 0, BitUtil.SIZE_OF_INT + META_DATA_LENGTH);
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
    }
}