using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
	/// <summary>
	/// Message to denote a response to a create static counter request.
	/// </summary>
	/// <seealso cref="ControlProtocolEvents"/>
	/// <pre>
	///   0                   1                   2                   3
	///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	///  |                         Correlation ID                        |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                           Counter ID                          |
	///  +---------------------------------------------------------------+
	/// </pre>/>
	public class StaticCounterFlyweight
	{
		/// <summary>
		/// Length of the header.
		/// </summary>
		public static readonly int LENGTH = BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_INT;

		private const int CORRELATION_ID_OFFSET = 0;
		private static readonly int CounterIdOffset = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;

		private IMutableDirectBuffer Buffer;
		private int Offset;

		/// <summary>
		/// Wrap the buffer at a given offset for updates.
		/// </summary>
		/// <param name="buffer"> to wrap </param>
		/// <param name="offset"> at which the message begins. </param>
		/// <returns> this for a fluent API. </returns>
		public StaticCounterFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
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
		public StaticCounterFlyweight CorrelationId(long correlationId)
		{
			Buffer.PutLong(Offset + CORRELATION_ID_OFFSET, correlationId);

			return this;
		}

		/// <summary>
		/// The counter id.
		/// </summary>
		/// <returns> counter id. </returns>
		public int CounterId()
		{
			return Buffer.GetInt(Offset + CounterIdOffset);
		}

		/// <summary>
		/// Set counter id field.
		/// </summary>
		/// <param name="counterId"> field value. </param>
		/// <returns> this for a fluent API. </returns>
		public StaticCounterFlyweight CounterId(int counterId)
		{
			Buffer.PutInt(Offset + CounterIdOffset, counterId);

			return this;
		}

		/// <summary>
		/// {@inheritDoc}
		/// </summary>
		public override string ToString()
		{
			return "StaticCounterFlyweight{" + 
			       "correlationId=" + CorrelationId() + 
			       ", counterId=" + CounterId() +
			       "}";
		}
	}
}