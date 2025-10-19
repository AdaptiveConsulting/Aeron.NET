using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron.Command
{
	/// <summary>
	/// Control message for getting next available session id from the media driver
	/// (<seealso cref="ControlProtocolEvents.GET_NEXT_AVAILABLE_SESSION_ID"/>).
	/// <pre>
	///   0                   1                   2                   3
	///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	///  |                          Client ID                            |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                    Command Correlation ID                     |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                         Stream Id                             |
	///  +---------------------------------------------------------------+
	/// </pre>
	/// </summary>
	public sealed class GetNextAvailableSessionIdMessageFlyweight : CorrelatedMessageFlyweight
	{
		private static readonly int StreamIdOffset = CorrelatedMessageFlyweight.LENGTH;

		/// <summary>
		/// Length of the header.
		/// </summary>
		public new static readonly int LENGTH = StreamIdOffset + SIZE_OF_INT;

		/// <summary>
		/// Wrap the buffer at a given offset for updates.
		/// </summary>
		/// <param name="buffer"> to wrap. </param>
		/// <param name="offset"> at which the message begins. </param>
		/// <returns> this for a fluent API. </returns>
		public new GetNextAvailableSessionIdMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
		{
			base.Wrap(buffer, offset);

			return this;
		}

		/// <summary>
		/// Get the stream id.
		/// </summary>
		/// <returns> the stream id. </returns>
		public int StreamId()
		{
			return buffer.GetInt(offset + StreamIdOffset);
		}

		/// <summary>
		/// Set the stream id.
		/// </summary>
		/// <param name="streamId"> the channel id. </param>
		/// <returns> this for a fluent API. </returns>
		public GetNextAvailableSessionIdMessageFlyweight StreamId(int streamId)
		{
			buffer.PutInt(offset + StreamIdOffset, streamId);

			return this;
		}

		/// <summary>
		/// Length of the message in bytes. Only valid after the channel is set.
		/// </summary>
		/// <returns> length of the message in bytes. </returns>
		public int Length()
		{
			return LENGTH;
		}

		/// <summary>
		/// Validate buffer length is long enough for message.
		/// </summary>
		/// <param name="msgTypeId"> type of message. </param>
		/// <param name="length"> of message in bytes to validate. </param>
		public new void ValidateLength(int msgTypeId, int length)
		{
			if (length < LENGTH)
			{
				throw new ControlProtocolException(
					ErrorCode.MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
			}
		}
	}
}