using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Represents the header of the data frame for accessing meta data fields.
    /// </summary>
    public class Header
    {
        private readonly int _positionBitsToShift;
        private int _initialTermId;
        private int _offset = 0;
        private IDirectBuffer _buffer;

        /// <summary>
        /// Construct a header that references a buffer for the log.
        /// </summary>
        /// <param name="initialTermId">       this stream started at. </param>
        /// <param name="positionBitsToShift"> for calculating positions. </param>
        public Header(int initialTermId, int positionBitsToShift)
        {
            this._initialTermId = initialTermId;
            this._positionBitsToShift = positionBitsToShift;
        }

        /// <summary>
        /// Get the current position to which the image has advanced on reading this message.
        /// </summary>
        /// <returns> the current position to which the image has advanced on reading this message. </returns>
        public long Position()
        {
            int resultingOffset = BitUtil.Align(TermOffset() + FrameLength(), FrameDescriptor.FRAME_ALIGNMENT);
            return LogBufferDescriptor.ComputePosition(TermId(), resultingOffset, _positionBitsToShift, _initialTermId);
        }

        /// <summary>
        /// Get the initial term id this stream started at.
        /// </summary>
        /// <returns> the initial term id this stream started at. </returns>
        public int InitialTermId()
        {
            return _initialTermId;
        }

        /// <summary>
        /// Set the initial term id this stream started at.
        /// </summary>
        /// <param name="initialTermId"> this stream started at. </param>
        public void InitialTermId(int initialTermId)
        {
            this._initialTermId = initialTermId;
        }

        /// <summary>
        /// Set the offset at which the header begins in the log.
        /// </summary>
        /// <param name="offset"> at which the header begins in the log. </param>
        public void Offset(int offset)
        {
            this._offset = offset;
        }

        /// <summary>
        /// The offset at which the frame begins.
        /// </summary>
        /// <returns> offset at which the frame begins. </returns>
        public int Offset()
        {
            return _offset;
        }

        /// <summary>
        /// The <seealso cref="IDirectBuffer"/> containing the header.
        /// </summary>
        /// <returns> <seealso cref="IDirectBuffer"/> containing the header. </returns>
        public IDirectBuffer Buffer()
        {
            return _buffer;
        }

        /// <summary>
        /// The <seealso cref="IDirectBuffer"/> containing the header.
        /// </summary>
        /// <param name="buffer"> <seealso cref="IDirectBuffer"/> containing the header. </param>
        public void Buffer(IDirectBuffer buffer)
        {
            _buffer = buffer;
        }

        /// <summary>
        /// The total length of the frame including the header.
        /// </summary>
        /// <returns> the total length of the frame including the header. </returns>
        public int FrameLength()
        {
            return _buffer.GetInt(_offset);
        }

        /// <summary>
        /// The session ID to which the frame belongs.
        /// </summary>
        /// <returns> the session ID to which the frame belongs. </returns>
        public int SessionId()
        {
            return _buffer.GetInt(_offset + DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// The stream ID to which the frame belongs.
        /// </summary>
        /// <returns> the stream ID to which the frame belongs. </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// The term ID to which the frame belongs.
        /// </summary>
        /// <returns> the term ID to which the frame belongs. </returns>
        public int TermId()
        {
            return _buffer.GetInt(_offset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// The offset in the term at which the frame begins. This will be the same as <seealso cref="Offset()"/>
        /// </summary>
        /// <returns> the offset in the term at which the frame begins. </returns>
        public int TermOffset()
        {
            return _offset;
        }

        /// <summary>
        /// The type of the the frame which should always be <seealso cref="DataHeaderFlyweight.HDR_TYPE_DATA"/>
        /// </summary>
        /// <returns> type of the the frame which should always be <seealso cref="DataHeaderFlyweight.HDR_TYPE_DATA"/> </returns>
        public int Type()
        {
            return _buffer.GetShort(_offset + HeaderFlyweight.TYPE_FIELD_OFFSET) & 0xFFFF;
        }

        /// <summary>
        /// The flags for this frame. Valid flags are <seealso cref="DataHeaderFlyweight.BEGIN_FLAG"/>
        /// and <seealso cref="DataHeaderFlyweight.END_FLAG"/>. A convenience flag <seealso cref="DataHeaderFlyweight.BEGIN_AND_END_FLAGS"/>
        /// can be used for both flags.
        /// </summary>
        /// <returns> the flags for this frame. </returns>
        public byte Flags()
        {
            return _buffer.GetByte(_offset + HeaderFlyweight.FLAGS_FIELD_OFFSET);
        }

        /// <summary>
        /// Get the value stored in the reserve space at the end of a data frame header.
        /// <para>
        /// Note: The value is in <seealso cref="ByteOrder.LittleEndian"/> format.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the value stored in the reserve space at the end of a data frame header. </returns>
        /// <seealso cref="DataHeaderFlyweight" />
        public long ReservedValue()
        {
            return _buffer.GetLong(_offset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET);
        }
    }
}