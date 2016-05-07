using System;

namespace Adaptive.Agrona.Concurrent.RingBuffer
{
    /// <summary>
    /// Description of the record structure for message framing in the a <seealso cref="RingBuffer"/>.
    /// </summary>
    public class RecordDescriptor
    {
        /// <summary>
        /// Header length made up of fields for length, type, and then the encoded message.
        /// <para>
        /// Writing of a positive record length signals the message recording is complete.
        /// <pre>
        ///   0                   1                   2                   3
        ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        ///  |R|                         Length                              |
        ///  +-+-------------------------------------------------------------+
        ///  |                            Type                               |
        ///  +---------------------------------------------------------------+
        ///  |                       Encoded Message                        ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        /// </pre>
        /// </para>
        /// </summary>
        public const int HeaderLength = BitUtil.SIZE_OF_INT*2;

        /// <summary>
        /// Alignment as a multiple of bytes for each record.
        /// </summary>
        public const int Alignment = HeaderLength;

        /// <summary>
        /// The offset from the beginning of a record at which the message length field begins.
        /// </summary>
        /// <param name="recordOffset"> beginning index of the record. </param>
        /// <returns> offset from the beginning of a record at which the type field begins. </returns>
        public static int LengthOffset(int recordOffset)
        {
            return recordOffset;
        }

        /// <summary>
        /// The offset from the beginning of a record at which the message type field begins.
        /// </summary>
        /// <param name="recordOffset"> beginning index of the record. </param>
        /// <returns> offset from the beginning of a record at which the type field begins. </returns>
        public static int TypeOffset(int recordOffset)
        {
            return recordOffset + BitUtil.SIZE_OF_INT;
        }

        /// <summary>
        /// The offset from the beginning of a record at which the encoded message begins.
        /// </summary>
        /// <param name="recordOffset"> beginning index of the record. </param>
        /// <returns> offset from the beginning of a record at which the encoded message begins. </returns>
        public static int EncodedMsgOffset(int recordOffset)
        {
            return recordOffset + HeaderLength;
        }

        /// <summary>
        /// Make a 64-bit header from the length and message type id.
        /// </summary>
        /// <param name="length">    length of the record </param>
        /// <param name="msgTypeId"> of the message stored in the record </param>
        /// <returns> the fields combined into a long. </returns>
        public static long MakeHeader(int length, int msgTypeId)
        {
            return ((msgTypeId & 0xFFFFFFFFL) << 32) | (length & 0xFFFFFFFFL);
        }

        /// <summary>
        /// Extract the record length field from a word representing the header.
        /// </summary>
        /// <param name="header"> containing both fields. </param>
        /// <returns> the length field from the header. </returns>
        public static int RecordLength(long header)
        {
            return (int) header;
        }

        public static int MessageTypeId(long header)
        {
            return (int) (long) ((ulong) header >> 32);
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
                string msg = $"Message type id must be greater than zero, msgTypeId={msgTypeId:D}";
                throw new ArgumentException(msg);
            }
        }
    }
}