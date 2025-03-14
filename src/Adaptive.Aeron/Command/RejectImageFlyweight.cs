using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
/// Control message to reject an image for a subscription.
/// 
/// <pre>
///   0                   1                   2                   3
///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///  |                          Client ID                            |
///  |                                                               |
///  +---------------------------------------------------------------+
///  |                       Correlation ID                          |
///  |                                                               |
///  +---------------------------------------------------------------+
///  |                    Image Correlation ID                       |
///  |                                                               |
///  +---------------------------------------------------------------+
///  |                           Position                            |
///  |                                                               |
///  +---------------------------------------------------------------+
///  |                        Reason Length                          |
///  +---------------------------------------------------------------+
///  |                        Reason (ASCII)                       ...
///  ...                                                             |
///  +---------------------------------------------------------------+
/// </pre>
/// </summary>
public class RejectImageFlyweight : CorrelatedMessageFlyweight
{
	private static readonly int ImageCorrelationIdFieldOffset = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
	private static readonly int PositionFieldOffset = ImageCorrelationIdFieldOffset + BitUtil.SIZE_OF_LONG;
	private static readonly int ReasonFieldOffset = PositionFieldOffset + BitUtil.SIZE_OF_LONG;
	private static readonly int MinimumSize = ReasonFieldOffset + BitUtil.SIZE_OF_INT;

	/// <summary>
	/// Wrap the buffer at a given offset for updates.
	/// </summary>
	/// <param name="buffer"> to wrap. </param>
	/// <param name="offset"> at which the message begins. </param>
	/// <returns> this for a fluent API. </returns>
	public new RejectImageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
	{
		base.Wrap(buffer, offset);
		return this;
	}

	/// <summary>
	/// Get image correlation id field.
	/// </summary>
	/// <returns> image correlation id field. </returns>
	public long ImageCorrelationId()
	{
		return buffer.GetLong(offset + ImageCorrelationIdFieldOffset);
	}

	/// <summary>
	/// Put image correlation id field.
	/// </summary>
	/// <param name="position"> new image correlation id value. </param>
	/// <returns> this for a fluent API. </returns>
	public RejectImageFlyweight ImageCorrelationId(long position)
	{
		buffer.PutLong(offset + ImageCorrelationIdFieldOffset, position);
		return this;
	}

	/// <summary>
	/// Get position field.
	/// </summary>
	/// <returns> position field. </returns>
	public long Position()
	{
		return buffer.GetLong(offset + PositionFieldOffset);
	}

	/// <summary>
	/// Put position field.
	/// </summary>
	/// <param name="position"> new position value. </param>
	/// <returns> this for a fluent API. </returns>
	public RejectImageFlyweight Position(long position)
	{
		buffer.PutLong(offset + PositionFieldOffset, position);
		return this;
	}

	/// <summary>
	/// Put reason field as ASCII. Include the reason length in the message.
	/// </summary>
	/// <param name="reason"> for invalidating the image. </param>
	/// <returns> this for a fluent API. </returns>
	public RejectImageFlyweight Reason(string reason)
	{
		buffer.PutStringAscii(offset + ReasonFieldOffset, reason);
		return this;
	}


	/// <summary>
	/// Get reason field as ASCII.
	/// </summary>
	/// <returns> reason for invalidating the image. </returns>
	public string Reason()
	{
		return buffer.GetStringAscii(offset + ReasonFieldOffset);
	}

	/// <summary>
	/// Length of the reason text.
	/// </summary>
	/// <returns> length of the reason text. </returns>
	public int ReasonBufferLength()
	{
		// This does make the assumption that the string is stored with the leading 4 bytes representing the length.
		return buffer.GetInt(offset + ReasonFieldOffset);
	}

	/// <summary>
	/// {@inheritDoc}
	/// </summary>
	public new RejectImageFlyweight ClientId(long clientId)
	{
		base.ClientId(clientId);
		return this;
	}

	/// <summary>
	/// {@inheritDoc}
	/// </summary>
	public new RejectImageFlyweight CorrelationId(long correlationId)
	{
		base.CorrelationId(correlationId);
		return this;
	}

	/// <summary>
	/// Compute the length of the message based on the reason supplied.
	/// </summary>
	/// <param name="reason"> message to be return to originator. </param>
	/// <returns> length of the message. </returns>
	public static int ComputeLength(string reason)
	{
		return MinimumSize + reason.Length;
	}

	/// <summary>
	/// Validate buffer length is long enough for message.
	/// </summary>
	/// <param name="msgTypeId"> type of message. </param>
	/// <param name="length"> of message in bytes to validate. </param>
	public new void ValidateLength(int msgTypeId, int length)
	{
		if (length < MinimumSize)
		{
			throw new ControlProtocolException(ErrorCode.MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
		}

		if (length < MinimumSize + ReasonBufferLength())
		{
			throw new ControlProtocolException(ErrorCode.MALFORMED_COMMAND, "command=" + msgTypeId + " too short: length=" + length);
		}
	}
}

}