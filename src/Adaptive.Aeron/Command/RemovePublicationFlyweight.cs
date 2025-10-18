using Adaptive.Agrona;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron.Command
{
	/// <summary>
	/// Control message for removing a Publication.
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
	///  |                       Registration ID                         |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                           Flags                               |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	/// </pre>
	/// </summary>
	public class RemovePublicationFlyweight : RemoveMessageFlyweight
	{
		private static readonly int FlagsFieldOffset = REGISTRATION_ID_FIELD_OFFSET + SIZE_OF_LONG;

		private const long FLAG_REVOKE = 0x1;

		/// <summary>
		/// Wrap the buffer at a given offset for updates.
		/// </summary>
		/// <param name="buffer"> to wrap. </param>
		/// <param name="offset"> at which the message begins. </param>
		/// <returns> this for a fluent API. </returns>
		public new RemovePublicationFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
		{
			base.Wrap(buffer, offset);

			return this;
		}

		/// <summary>
		/// Length of the message in bytes.
		/// </summary>
		/// <returns> length of the message in bytes. </returns>
		public new static int Length()
		{
			return RemoveMessageFlyweight.Length() + SIZE_OF_LONG;
		}

		/// <summary>
		/// Whether or not the message contains the flags field.
		/// </summary>
		/// <param name="messageLength"> the length of the message. </param>
		/// <returns> true if the flags field can be read. </returns>
		public bool FlagsFieldIsValid(int messageLength)
		{
			return messageLength >= FlagsFieldOffset + SIZE_OF_LONG;
		}

		/// <summary>
		/// Get the value of the revoke field.
		/// </summary>
		/// <returns> revoked. </returns>
		public bool Revoke()
		{
			return (buffer.GetLong(offset + FlagsFieldOffset) & FLAG_REVOKE) > 0;
		}

		/// <summary>
		/// Whether or not the message contains the set revoke flag.
		/// </summary>
		/// <param name="messageLength"> the length of the message. </param>
		/// <returns> true if the flags field is present AND the revoked flag is set. </returns>
		public bool Revoke(int messageLength)
		{
			return FlagsFieldIsValid(messageLength) && Revoke();
		}

		/// <summary>
		/// Set the value of the revoke field.
		/// </summary>
		/// <param name="revoke"> field value. </param>
		/// <returns> this for a fluent API. </returns>
		public RemovePublicationFlyweight Revoke(bool revoke)
		{
			long flags = buffer.GetLong(offset + FlagsFieldOffset);

			if (revoke)
			{
				flags |= FLAG_REVOKE;
			}
			else
			{
				flags &= ~FLAG_REVOKE;
			}

			buffer.PutLong(offset + FlagsFieldOffset, flags);

			return this;
		}
	}
}