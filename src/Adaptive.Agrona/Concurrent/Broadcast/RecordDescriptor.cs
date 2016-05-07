using System;

namespace Adaptive.Agrona.Concurrent.Broadcast
{
    /// <summary>
    /// Description of the structure for a record in the broadcast buffer.
    /// All messages are stored in records with the following format.
    /// 
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |R|                        Length                               |
    ///  +-+-------------------------------------------------------------+
    ///  |                           Type                                |
    ///  +---------------------------------------------------------------+
    ///  |                       Encoded Message                        ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// 
    /// (R) bits are reserved.
    /// </summary>
    public class RecordDescriptor
    {
        /// <summary>
        /// Message type is padding to prevent fragmentation in the buffer. </summary>
        public const int PaddingMsgTypeID = -1;

        /// <summary>
        /// Offset within the record at which the record length field begins. </summary>
        public const int LengthOffset = 0;

        /// <summary>
        /// Offset within the record at which the message type field begins. </summary>
        public const int TypeOffset = LengthOffset + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Length of the record header in bytes. </summary>
        public const int HeaderLength = BitUtil.SIZE_OF_INT*2;

        /// <summary>
        /// Alignment as a multiple of bytes for each record. </summary>
        public const int RecordAlignment = HeaderLength;

        /// <summary>
        /// Calculate the maximum supported message length for a buffer of given capacity.
        /// </summary>
        /// <param name="capacity"> of the log buffer. </param>
        /// <returns> the maximum supported size for a message. </returns>
        public static int CalculateMaxMessageLength(int capacity)
        {
            return capacity/8;
        }

        /// <summary>
        /// The buffer offset at which the message length field begins.
        /// </summary>
        /// <param name="recordOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the message length field begins. </returns>
        public static int GetLengthOffset(int recordOffset)
        {
            return recordOffset + LengthOffset;
        }

        /// <summary>
        /// The buffer offset at which the message type field begins.
        /// </summary>
        /// <param name="recordOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the message type field begins. </returns>
        public static int GetTypeOffset(int recordOffset)
        {
            return recordOffset + TypeOffset;
        }

        /// <summary>
        /// The buffer offset at which the encoded message begins.
        /// </summary>
        /// <param name="recordOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the encoded message begins. </returns>
        public static int GetMsgOffset(int recordOffset)
        {
            return recordOffset + HeaderLength;
        }

        /// <summary>
        /// Check that and message id is in the valid range.
        /// </summary>
        /// <param name="msgTypeId"> to be checked. </param>
        /// <exception cref="ArgumentException"> if the id is not in the valid range. </exception>
        public static void CheckTypeId(int msgTypeId)
        {
            if (msgTypeId < 1)
            {
                var msg = $"Type id must be greater than zero, msgTypeId={msgTypeId:D}";
                throw new ArgumentException(msg);
            }
        }
    }
}