using Adaptive.Agrona;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron.Command
{
	/// <summary>
	/// Message to denote a response to get next session id command
	/// (<seealso cref="ControlProtocolEvents.ON_NEXT_AVAILABLE_SESSION_ID"/>).
	/// </summary>
	/// <seealso cref="ControlProtocolEvents"/>
	/// <pre>
	///   0                   1                   2                   3
	///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	///  |                         Correlation ID                        |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                         Next Session ID                        |
	///  +---------------------------------------------------------------+
	/// </pre>
	/// <remarks>Since 1.49.0</remarks>
	public class NextAvailableSessionIdFlyweight
	{
		private const int CORRELATION_ID_OFFSET = 0;
		private static readonly int SessionIdOffset = CORRELATION_ID_OFFSET + SIZE_OF_LONG;

		/// <summary>
		/// Length of the header.
		/// </summary>
		public static readonly int LENGTH = SessionIdOffset + SIZE_OF_INT;

		private IMutableDirectBuffer Buffer;
		private int Offset;

		/// <summary>
		/// Wrap the buffer at a given offset for updates.
		/// </summary>
		/// <param name="buffer"> to wrap </param>
		/// <param name="offset"> at which the message begins. </param>
		/// <returns> this for a fluent API. </returns>
		public NextAvailableSessionIdFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
		{
			this.Buffer = buffer;
			this.Offset = offset;

			return this;
		}

		/// <summary>
		/// Get the correlation id field.
		/// </summary>
		/// <returns> correlation id field. </returns>
		public long CorrelationId()
		{
			return Buffer.GetLong(Offset + CORRELATION_ID_OFFSET);
		}

		/// <summary>
		/// Set the correlation id field.
		/// </summary>
		/// <param name="correlationId"> field value. </param>
		/// <returns> this for a fluent API. </returns>
		public NextAvailableSessionIdFlyweight CorrelationId(long correlationId)
		{
			Buffer.PutLong(Offset + CORRELATION_ID_OFFSET, correlationId);

			return this;
		}

		/// <summary>
		/// The session id.
		/// </summary>
		/// <returns> session id. </returns>
		public int NextSessionId()
		{
			return Buffer.GetInt(Offset + SessionIdOffset);
		}

		/// <summary>
		/// Set session id field.
		/// </summary>
		/// <param name="sessionId"> field value. </param>
		/// <returns> this for a fluent API. </returns>
		public NextAvailableSessionIdFlyweight NextSessionId(int sessionId)
		{
			Buffer.PutInt(Offset + SessionIdOffset, sessionId);

			return this;
		}

		/// <summary>
		/// {@inheritDoc}
		/// </summary>
		public override string ToString()
		{
			return "NextSessionIdFlyweight{" + 
			       "correlationId=" + CorrelationId() + 
			       ", sessionId=" + NextSessionId() +
			       "}";
		}
	}
}