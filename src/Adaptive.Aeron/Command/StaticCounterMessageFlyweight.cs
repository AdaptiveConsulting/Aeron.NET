using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
	/// <summary>
	/// Message to get or create a new static counter.
	/// <para>
	/// <b>Note:</b> Layout should be SBE 2.0 compliant so that the label length is aligned.
	/// 
	/// </para>
	/// </summary>
	/// <seealso cref="ControlProtocolEvents"/>
	/// <pre>
	///   0                   1                   2                   3
	///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	///  |                          Client ID                            |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                        Correlation ID                         |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                        Registration ID                        |
	///  |                                                               |
	///  +---------------------------------------------------------------+
	///  |                        Counter Type ID                        |
	///  +---------------------------------------------------------------+
	///  |                           Key Length                          |
	///  +---------------------------------------------------------------+
	///  |                           Key Buffer                         ...
	/// ...                                                              |
	///  +---------------------------------------------------------------+
	///  |                          Label Length                         |
	///  +---------------------------------------------------------------+
	///  |                          Label (ASCII)                       ...
	/// ...                                                              |
	///  +---------------------------------------------------------------+
	/// </pre>/>
	public class StaticCounterMessageFlyweight : CorrelatedMessageFlyweight
	{
		private static readonly int RegistrationIdFieldOffset = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
		private static readonly int CounterTypeIdFieldOffset = RegistrationIdFieldOffset + BitUtil.SIZE_OF_LONG;
		private static readonly int KeyLengthOffset = CounterTypeIdFieldOffset + BitUtil.SIZE_OF_INT;
		internal static readonly int KEY_BUFFER_OFFSET = KeyLengthOffset + BitUtil.SIZE_OF_INT;
		private static readonly int MinimumLength = KEY_BUFFER_OFFSET + BitUtil.SIZE_OF_INT;

		/// <summary>
		/// Wrap the buffer at a given offset for updates.
		/// </summary>
		/// <param name="buffer"> to wrap. </param>
		/// <param name="offset"> at which the message begins. </param>
		/// <returns> this for a fluent API. </returns>
		public new StaticCounterMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
		{
			base.Wrap(buffer, offset);

			return this;
		}

		/// <summary>
		/// Get registration id field.
		/// </summary>
		/// <returns> registration id field. </returns>
		public long RegistrationId()
		{
			return buffer.GetLong(offset + RegistrationIdFieldOffset);
		}

		/// <summary>
		/// Set counter registration id field.
		/// </summary>
		/// <param name="registrationId"> field value. </param>
		/// <returns> this for a fluent API. </returns>
		public StaticCounterMessageFlyweight RegistrationId(long registrationId)
		{
			buffer.PutLong(offset + RegistrationIdFieldOffset, registrationId);
			return this;
		}

		/// <summary>
		/// Get type id field.
		/// </summary>
		/// <returns> type id field. </returns>
		public int TypeId()
		{
			return buffer.GetInt(offset + CounterTypeIdFieldOffset);
		}

		/// <summary>
		/// Set counter type id field.
		/// </summary>
		/// <param name="typeId"> field value. </param>
		/// <returns> this for a fluent API. </returns>
		public StaticCounterMessageFlyweight TypeId(int typeId)
		{
			buffer.PutInt(offset + CounterTypeIdFieldOffset, typeId);
			return this;
		}

		/// <summary>
		/// Relative offset of the key buffer.
		/// </summary>
		/// <returns> relative offset of the key buffer. </returns>
		public int KeyBufferOffset()
		{
			return KEY_BUFFER_OFFSET;
		}

		/// <summary>
		/// Length of the key buffer in bytes.
		/// </summary>
		/// <returns> length of key buffer in bytes. </returns>
		public int KeyBufferLength()
		{
			return buffer.GetInt(offset + KeyLengthOffset);
		}

		/// <summary>
		/// Fill the key buffer.
		/// </summary>
		/// <param name="keyBuffer"> containing the optional key for the counter. </param>
		/// <param name="keyOffset"> within the keyBuffer at which the key begins. </param>
		/// <param name="keyLength"> of the key in the keyBuffer. </param>
		/// <returns> this for a fluent API. </returns>
		public StaticCounterMessageFlyweight KeyBuffer(IDirectBuffer keyBuffer, int keyOffset, int keyLength)
		{
			buffer.PutInt(offset + KeyLengthOffset, keyLength);
			if (null != keyBuffer && keyLength > 0)
			{
				buffer.PutBytes(offset + KEY_BUFFER_OFFSET, keyBuffer, keyOffset, keyLength);
			}

			return this;
		}

		/// <summary>
		/// Relative offset of label buffer.
		/// </summary>
		/// <returns> relative offset of label buffer. </returns>
		public int LabelBufferOffset()
		{
			return LabelLengthOffset() + BitUtil.SIZE_OF_INT;
		}

		/// <summary>
		/// Length of label buffer in bytes.
		/// </summary>
		/// <returns> length of label buffer in bytes. </returns>
		public int LabelBufferLength()
		{
			return buffer.GetInt(offset + LabelLengthOffset());
		}

		/// <summary>
		/// Fill the label buffer.
		/// </summary>
		/// <param name="labelBuffer"> containing the mandatory label for the counter. </param>
		/// <param name="labelOffset"> within the labelBuffer at which the label begins. </param>
		/// <param name="labelLength"> of the label in the labelBuffer. </param>
		/// <returns> this for a fluent API. </returns>
		public StaticCounterMessageFlyweight LabelBuffer(IDirectBuffer labelBuffer, int labelOffset, int labelLength)
		{
			int labelLengthOffset = LabelLengthOffset();
			buffer.PutInt(offset + labelLengthOffset, labelLength);
			if (null != labelBuffer && labelLength > 0)
			{
				buffer.PutBytes(offset + labelLengthOffset + BitUtil.SIZE_OF_INT, labelBuffer, labelOffset, labelLength);
			}

			return this;
		}

		/// <summary>
		/// Fill the label.
		/// </summary>
		/// <param name="label"> for the counter. </param>
		/// <returns> this for a fluent API. </returns>
		public StaticCounterMessageFlyweight Label(string label)
		{
			buffer.PutStringAscii(offset + LabelLengthOffset(), label);

			return this;
		}

		/// <summary>
		/// Get the length of the current message.
		/// <para>
		/// NB: must be called after the data is written in order to be accurate.
		/// 
		/// </para>
		/// </summary>
		/// <returns> the length of the current message </returns>
		public int Length()
		{
			int labelOffset = LabelLengthOffset();
			return labelOffset + BitUtil.SIZE_OF_INT + LabelBufferLength();
		}

		/// <summary>
		/// Validate buffer length is long enough for message.
		/// </summary>
		/// <param name="msgTypeId"> type of message. </param>
		/// <param name="length">    of message in bytes to validate. </param>
		public new void ValidateLength(int msgTypeId, int length)
		{
			if (length < MinimumLength)
			{
				throw new ControlProtocolException(ErrorCode.MALFORMED_COMMAND,
					"command=" + msgTypeId + " too short: length=" + length);
			}

			int labelOffset = LabelLengthOffset();
			if ((length - labelOffset) < BitUtil.SIZE_OF_INT)
			{
				throw new ControlProtocolException(ErrorCode.MALFORMED_COMMAND,
					"command=" + msgTypeId + " too short for key: length=" + length);
			}

			int encodedLength = Length();
			if (length < encodedLength)
			{
				throw new ControlProtocolException(ErrorCode.MALFORMED_COMMAND,
					"command=" + msgTypeId + " too short for label: length=" + length + " encodedLength=" +
					encodedLength);
			}
		}

		/// <summary>
		/// Compute the length of the command message given key and label length.
		/// </summary>
		/// <param name="keyLength">   to be appended. </param>
		/// <param name="labelLength"> to be appended. </param>
		/// <returns> the length of the command message given key and label length. </returns>
		public static int ComputeLength(int keyLength, int labelLength)
		{
			return MinimumLength +  BitUtil.Align(keyLength,  BitUtil.SIZE_OF_INT) +  BitUtil.SIZE_OF_INT + labelLength;
		}

		/// <summary>
		/// {@inheritDoc}
		/// </summary>
		public override string ToString()
		{
			return "StaticCounterMessageFlyweight{" + "clientId=" + ClientId() + ", correlationId=" + CorrelationId() +
			       ", registrationId=" + RegistrationId() + ", typeId=" + TypeId() + ", keyBufferOffset=" +
			       KeyBufferOffset() + ", keyBufferLength=" + KeyBufferLength() + ", labelLengthOffset=" +
			       LabelLengthOffset() + ", labelBufferOffset=" + LabelBufferOffset() + ", labelBufferLength=" +
			       LabelBufferLength() + ", length=" + Length() + "}";
		}

		private int LabelLengthOffset()
		{
			return KEY_BUFFER_OFFSET + BitUtil.Align(KeyBufferLength(),  BitUtil.SIZE_OF_INT);
		}
	}
}