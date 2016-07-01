using System.Runtime.CompilerServices;
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

        public Header()
        {
        }

        /// <summary>
        /// Construct a header that references a buffer for the log.
        /// </summary>
        /// <param name="initialTermId">       this stream started at. </param>
        /// <param name="positionBitsToShift"> for calculating positions. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Header(int initialTermId, int positionBitsToShift)
        {
            _initialTermId = initialTermId;
            _positionBitsToShift = positionBitsToShift;
        }

        /// <summary>
        /// Get the current position to which the image has advanced on reading this message.
        /// </summary>
        /// <returns> the current position to which the image has advanced on reading this message. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Position()
        {
            var resultingOffset = BitUtil.Align(TermOffset + FrameLength, FrameDescriptor.FRAME_ALIGNMENT);
            return LogBufferDescriptor.ComputePosition(TermId, resultingOffset, _positionBitsToShift, _initialTermId);
        }

        /// <summary>
        /// Get the initial term id this stream started at.
        /// </summary>
        /// <returns> the initial term id this stream started at. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int InitialTermId()
        {
            return _initialTermId;
        }

        /// <summary>
        /// Set the initial term id this stream started at.
        /// </summary>
        /// <param name="initialTermId"> this stream started at. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialTermId(int initialTermId)
        {
            _initialTermId = initialTermId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetBuffer(IDirectBuffer buffer, int offset)
        {
            Buffer = buffer;
            Offset = offset;
        }

        /// <summary>
        /// The offset at which the frame begins.
        /// </summary>
        public int Offset { get; private set; }
        
        /// <summary>
        /// The <seealso cref="IDirectBuffer"/> containing the header.
        /// </summary>
        public IDirectBuffer Buffer { get; private set; }

        /// <summary>
        /// The total length of the frame including the header.
        /// </summary>
        /// <returns> the total length of the frame including the header. </returns>
        public int FrameLength => Buffer.GetInt(Offset);

        /// <summary>
        /// The session ID to which the frame belongs.
        /// </summary>
        /// <returns> the session ID to which the frame belongs. </returns>
        public int SessionId => Buffer.GetInt(Offset + DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET);

        /// <summary>
        /// The stream ID to which the frame belongs.
        /// </summary>
        /// <returns> the stream ID to which the frame belongs. </returns>
        public int StreamId => Buffer.GetInt(Offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET);

        /// <summary>
        /// The term ID to which the frame belongs.
        /// </summary>
        /// <returns> the term ID to which the frame belongs. </returns>
        public int TermId => Buffer.GetInt(Offset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET);

        /// <summary>
        /// The offset in the term at which the frame begins. This will be the same as <seealso cref="Offset()"/>
        /// </summary>
        /// <returns> the offset in the term at which the frame begins. </returns>
        public int TermOffset => Offset;

        /// <summary>
        /// The type of the the frame which should always be <seealso cref="DataHeaderFlyweight.HDR_TYPE_DATA"/>
        /// </summary>
        /// <returns> type of the the frame which should always be <seealso cref="DataHeaderFlyweight.HDR_TYPE_DATA"/> </returns>
        public int Type => Buffer.GetShort(Offset + HeaderFlyweight.TYPE_FIELD_OFFSET) & 0xFFFF;

        /// <summary>
        /// The flags for this frame. Valid flags are <seealso cref="DataHeaderFlyweight.BEGIN_FLAG"/>
        /// and <seealso cref="DataHeaderFlyweight.END_FLAG"/>. A convenience flag <seealso cref="DataHeaderFlyweight.BEGIN_AND_END_FLAGS"/>
        /// can be used for both flags.
        /// </summary>
        /// <returns> the flags for this frame. </returns>
        public virtual byte Flags => Buffer.GetByte(Offset + HeaderFlyweight.FLAGS_FIELD_OFFSET);

        /// <summary>
        /// Get the value stored in the reserve space at the end of a data frame header.
        /// <para>
        /// Note: The value is in <seealso cref="ByteOrder.LittleEndian"/> format.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the value stored in the reserve space at the end of a data frame header. </returns>
        /// <seealso cref="DataHeaderFlyweight" />
        public long ReservedValue => Buffer.GetLong(Offset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET);
    }
}