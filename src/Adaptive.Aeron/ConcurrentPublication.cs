using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;
using static Adaptive.Aeron.LogBuffer.LogBufferDescriptor;
using static Adaptive.Aeron.Protocol.DataHeaderFlyweight;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron
{
	/// <summary>
	/// Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. <seealso cref="Publication"/>s
	/// are created via the <seealso cref="Aeron.AddPublication(string, int)"/> method, and messages are sent via one of the
	/// <seealso cref="Publication.Offer(UnsafeBuffer)"/> methods, or a <seealso cref="TryClaim(int, BufferClaim)"/> and <seealso cref="BufferClaim.Commit()"/>
	/// method combination.
	/// <para>
	/// The APIs for tryClaim and offer are non-blocking and thread safe.
	/// </para>
	/// <para>
	/// <b>Note:</b> Instances are threadsafe and can be shared between publishing threads.
	/// 
	/// </para>
	/// </summary>
	/// <seealso cref="Aeron.AddPublication(string, int)"></seealso>
	/// <seealso cref="BufferClaim"></seealso>
	public sealed class ConcurrentPublication : Publication
	{
		internal ConcurrentPublication(
			ClientConductor clientConductor,
			string channel,
			int streamId,
			int sessionId,
			IReadablePosition positionLimit,
			int channelStatusId,
			LogBuffers logBuffers,
			long originalRegistrationId,
			long registrationId)
			: base(
				clientConductor,
				channel,
				streamId,
				sessionId,
				positionLimit,
				channelStatusId,
				logBuffers,
				originalRegistrationId,
				registrationId
			)
		{
		}

		/// <inheritdoc />
		public override long AvailableWindow
		{
			get
			{
				if (_isClosed)
				{
					return CLOSED;
				}

				return _positionLimit.GetVolatile() - Position;
			}
		}

		/// <inheritdoc />
		public override long Offer(IDirectBuffer buffer, int offset, int length,
			ReservedValueSupplier reservedValueSupplier = null)
		{
			long newPosition = CLOSED;
			if (!_isClosed)
			{
				long limit = _positionLimit.GetVolatile();
				int termCount = ActiveTermCount(_logMetaDataBuffer);
				int index = IndexByTermCount(termCount);
				UnsafeBuffer termBuffer = _termBuffers[index];
				int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
				long rawTail = _logMetaDataBuffer.GetLongVolatile(tailCounterOffset);
				int termOffset = TermOffset(rawTail, termBuffer.Capacity);
				int termId = TermId(rawTail);

				if (termCount != (termId - InitialTermId))
				{
					return ADMIN_ACTION;
				}

				long position = ComputePosition(termId, termOffset, PositionBitsToShift, InitialTermId);
				
				if (position < limit)
				{
					if (length <= MaxPayloadLength)
					{
						CheckPositiveLength(length);
						newPosition = AppendUnfragmentedMessage(
							termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
					}
					else
					{
						CheckMaxMessageLength(length);
						newPosition = AppendFragmentedMessage(
							termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
					}
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}


		/// <inheritdoc />
		public override long Offer(IDirectBuffer bufferOne, int offsetOne, int lengthOne, IDirectBuffer bufferTwo,
			int offsetTwo, int lengthTwo, ReservedValueSupplier reservedValueSupplier = null)
		{
			long newPosition = CLOSED;
			if (!_isClosed)
			{
				long limit = _positionLimit.GetVolatile();
				int termCount = ActiveTermCount(_logMetaDataBuffer);
				int index = IndexByTermCount(termCount);
				UnsafeBuffer termBuffer = _termBuffers[index];
				int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
				long rawTail = _logMetaDataBuffer.GetLongVolatile(tailCounterOffset);
				int termOffset = TermOffset(rawTail, termBuffer.Capacity);
				int termId = TermId(rawTail);

				if (termCount != (termId - InitialTermId))
				{
					return ADMIN_ACTION;
				}

				long position = ComputePosition(termId, termOffset, PositionBitsToShift, InitialTermId);

				int length = ValidateAndComputeLength(lengthOne, lengthTwo);
				if (position < limit)
				{
					if (length <= MaxPayloadLength)
					{
						newPosition = AppendUnfragmentedMessage(
							termBuffer,
							tailCounterOffset,
							bufferOne,
							offsetOne,
							lengthOne,
							bufferTwo,
							offsetTwo,
							lengthTwo,
							reservedValueSupplier);
					}
					else
					{
						CheckMaxMessageLength(length);
						newPosition = AppendFragmentedMessage(
							termBuffer,
							tailCounterOffset,
							bufferOne,
							offsetOne,
							lengthOne,
							bufferTwo,
							offsetTwo,
							lengthTwo,
							reservedValueSupplier);
					}
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}

		/// <inheritdoc />
		public override long Offer(DirectBufferVector[] vectors, ReservedValueSupplier reservedValueSupplier = null)
		{
			int length = DirectBufferVector.ValidateAndComputeLength(vectors);
			long newPosition = CLOSED;

			if (!_isClosed)
			{
				long limit = _positionLimit.GetVolatile();
				int termCount = ActiveTermCount(_logMetaDataBuffer);
				int index = IndexByTermCount(termCount);
				UnsafeBuffer termBuffer = _termBuffers[index];
				int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
				long rawTail = _logMetaDataBuffer.GetLongVolatile(tailCounterOffset);
				int termOffset = TermOffset(rawTail, termBuffer.Capacity);
				int termId = TermId(rawTail);

				if (termCount != (termId - InitialTermId))
				{
					return ADMIN_ACTION;
				}

				long position = ComputePosition(termId, termOffset, PositionBitsToShift, InitialTermId);

				if (position < limit)
				{
					if (length <= MaxPayloadLength)
					{
						newPosition = AppendUnfragmentedMessage(
							termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
					}
					else
					{
						CheckMaxMessageLength(length);
						newPosition = AppendFragmentedMessage(
							termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
					}
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}


		/// <inheritdoc />
		public override long TryClaim(int length, BufferClaim bufferClaim)
		{
			CheckPayloadLength(length);
			long newPosition = CLOSED;

			if (!_isClosed)
			{
				long limit = _positionLimit.GetVolatile();
				int termCount = ActiveTermCount(_logMetaDataBuffer);
				int index = IndexByTermCount(termCount);
				UnsafeBuffer termBuffer = _termBuffers[index];
				int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
				long rawTail = _logMetaDataBuffer.GetLongVolatile(tailCounterOffset);
				int termOffset = TermOffset(rawTail, termBuffer.Capacity);
				int termId = TermId(rawTail);

				if (termCount != (termId - InitialTermId))
				{
					return ADMIN_ACTION;
				}

				long position = ComputePosition(termId, termOffset, PositionBitsToShift, InitialTermId);

				if (position < limit)
				{
					newPosition = Claim(termBuffer, tailCounterOffset, length, bufferClaim);
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}

		private long AppendUnfragmentedMessage(UnsafeBuffer termBuffer, int tailCounterOffset, IDirectBuffer buffer,
			int offset, int length, ReservedValueSupplier reservedValueSupplier)
		{
			int frameLength = length + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			long rawTail = _logMetaDataBuffer.GetAndAddLong(tailCounterOffset, alignedLength);
			int termId = TermId(rawTail);
			int termOffset = TermOffset(rawTail, termLength);

			int resultingOffset = termOffset + alignedLength;
			long position = ComputePosition(termId, resultingOffset, PositionBitsToShift, InitialTermId);
			if (resultingOffset > termLength)
			{
				return HandleEndOfLog(termBuffer, termLength, termId, termOffset, position);
			}
			else
			{
				_headerWriter.Write(termBuffer, termOffset, frameLength, termId);
				termBuffer.PutBytes(termOffset + HEADER_LENGTH, buffer, offset, length);

				if (null != reservedValueSupplier)
				{
					long reservedValue = reservedValueSupplier(termBuffer, termOffset, frameLength);
					termBuffer.PutLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
				}

				FrameLengthOrdered(termBuffer, termOffset, frameLength);
			}

			return position;
		}

		private long AppendFragmentedMessage(UnsafeBuffer termBuffer, int tailCounterOffset, IDirectBuffer buffer,
			int offset, int length, ReservedValueSupplier reservedValueSupplier)
		{
			int framedLength = ComputeFragmentedFrameLength(length, MaxPayloadLength);
			int termLength = termBuffer.Capacity;

			long rawTail = _logMetaDataBuffer.GetAndAddLong(tailCounterOffset, framedLength);
			int termId = TermId(rawTail);
			int termOffset = TermOffset(rawTail, termLength);

			int resultingOffset = termOffset + framedLength;
			long position = ComputePosition(termId, resultingOffset, PositionBitsToShift, InitialTermId);
			if (resultingOffset > termLength)
			{
				return HandleEndOfLog(termBuffer, termLength, termId, termOffset, position);
			}
			else
			{
				int frameOffset = termOffset;
				byte flags = BEGIN_FRAG_FLAG;
				int remaining = length;

				do
				{
					int bytesToWrite = Math.Min(remaining, MaxPayloadLength);
					int frameLength = bytesToWrite + HEADER_LENGTH;
					int alignedLength = Align(frameLength, FRAME_ALIGNMENT);

					_headerWriter.Write(termBuffer, frameOffset, frameLength, termId);
					termBuffer.PutBytes(frameOffset + HEADER_LENGTH, buffer, offset + (length - remaining),
						bytesToWrite);

					if (remaining <= MaxPayloadLength)
					{
						flags |= END_FRAG_FLAG;
					}

					FrameFlags(termBuffer, frameOffset, flags);

					if (null != reservedValueSupplier)
					{
						long reservedValue = reservedValueSupplier(termBuffer, frameOffset, frameLength);
						termBuffer.PutLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
					}

					FrameLengthOrdered(termBuffer, frameOffset, frameLength);

					flags = 0;
					frameOffset += alignedLength;
					remaining -= bytesToWrite;
				} while (remaining > 0);
			}

			return position;
		}

		private long AppendUnfragmentedMessage(UnsafeBuffer termBuffer, int tailCounterOffset, IDirectBuffer bufferOne,
			int offsetOne, int lengthOne, IDirectBuffer bufferTwo, int offsetTwo, int lengthTwo,
			ReservedValueSupplier reservedValueSupplier)
		{
			int frameLength = lengthOne + lengthTwo + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			long rawTail = _logMetaDataBuffer.GetAndAddLong(tailCounterOffset, alignedLength);
			int termId = TermId(rawTail);
			int termOffset = TermOffset(rawTail, termLength);

			int resultingOffset = termOffset + alignedLength;
			long position = ComputePosition(termId, resultingOffset, PositionBitsToShift, InitialTermId);
			if (resultingOffset > termLength)
			{
				return HandleEndOfLog(termBuffer, termLength, termId, termOffset, position);
			}
			else
			{
				_headerWriter.Write(termBuffer, termOffset, frameLength, termId);
				termBuffer.PutBytes(termOffset + HEADER_LENGTH, bufferOne, offsetOne, lengthOne);
				termBuffer.PutBytes(termOffset + HEADER_LENGTH + lengthOne, bufferTwo, offsetTwo, lengthTwo);

				if (null != reservedValueSupplier)
				{
					long reservedValue = reservedValueSupplier(termBuffer, termOffset, frameLength);
					termBuffer.PutLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
				}

				FrameLengthOrdered(termBuffer, termOffset, frameLength);
			}

			return position;
		}

		private long AppendFragmentedMessage(UnsafeBuffer termBuffer, int tailCounterOffset, IDirectBuffer bufferOne,
			int offsetOne, int lengthOne, IDirectBuffer bufferTwo, int offsetTwo, int lengthTwo,
			ReservedValueSupplier reservedValueSupplier)
		{
			int length = lengthOne + lengthTwo;
			int framedLength = ComputeFragmentedFrameLength(length, MaxPayloadLength);
			int termLength = termBuffer.Capacity;

			long rawTail = _logMetaDataBuffer.GetAndAddLong(tailCounterOffset, framedLength);
			int termId = TermId(rawTail);
			int termOffset = TermOffset(rawTail, termLength);

			int resultingOffset = termOffset + framedLength;
			long position = ComputePosition(termId, resultingOffset, PositionBitsToShift, InitialTermId);
			if (resultingOffset > termLength)
			{
				return HandleEndOfLog(termBuffer, termLength, termId, termOffset, position);
			}
			else
			{
				int frameOffset = termOffset;
				byte flags = BEGIN_FRAG_FLAG;
				int remaining = length;
				int positionOne = 0;
				int positionTwo = 0;

				do
				{
					int bytesToWrite = Math.Min(remaining, MaxPayloadLength);
					int frameLength = bytesToWrite + HEADER_LENGTH;
					int alignedLength = Align(frameLength, FRAME_ALIGNMENT);

					_headerWriter.Write(termBuffer, frameOffset, frameLength, termId);

					int bytesWritten = 0;
					int payloadOffset = frameOffset + HEADER_LENGTH;
					do
					{
						int remainingOne = lengthOne - positionOne;
						if (remainingOne > 0)
						{
							int numBytes = Math.Min(bytesToWrite - bytesWritten, remainingOne);
							termBuffer.PutBytes(payloadOffset, bufferOne, offsetOne + positionOne, numBytes);

							bytesWritten += numBytes;
							payloadOffset += numBytes;
							positionOne += numBytes;
						}
						else
						{
							int numBytes = Math.Min(bytesToWrite - bytesWritten, lengthTwo - positionTwo);
							termBuffer.PutBytes(payloadOffset, bufferTwo, offsetTwo + positionTwo, numBytes);

							bytesWritten += numBytes;
							payloadOffset += numBytes;
							positionTwo += numBytes;
						}
					} while (bytesWritten < bytesToWrite);

					if (remaining <= MaxPayloadLength)
					{
						flags |= END_FRAG_FLAG;
					}

					FrameFlags(termBuffer, frameOffset, flags);

					if (null != reservedValueSupplier)
					{
						long reservedValue = reservedValueSupplier(termBuffer, frameOffset, frameLength);
						termBuffer.PutLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
					}

					FrameLengthOrdered(termBuffer, frameOffset, frameLength);

					flags = 0;
					frameOffset += alignedLength;
					remaining -= bytesToWrite;
				} while (remaining > 0);
			}

			return position;
		}

		private long AppendUnfragmentedMessage(UnsafeBuffer termBuffer, int tailCounterOffset,
			DirectBufferVector[] vectors, int length, ReservedValueSupplier reservedValueSupplier)
		{
			int frameLength = length + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			long rawTail = _logMetaDataBuffer.GetAndAddLong(tailCounterOffset, alignedLength);
			int termId = TermId(rawTail);
			int termOffset = TermOffset(rawTail, termLength);

			int resultingOffset = termOffset + alignedLength;
			long position = ComputePosition(termId, resultingOffset, PositionBitsToShift, InitialTermId);
			if (resultingOffset > termLength)
			{
				return HandleEndOfLog(termBuffer, termLength, termId, termOffset, position);
			}
			else
			{
				_headerWriter.Write(termBuffer, termOffset, frameLength, termId);

				int offset = termOffset + HEADER_LENGTH;
				foreach (DirectBufferVector vector in vectors)
				{
					termBuffer.PutBytes(offset, vector.Buffer(), vector.Offset(), vector.Length());
					offset += vector.Length();
				}

				if (null != reservedValueSupplier)
				{
					long reservedValue = reservedValueSupplier(termBuffer, termOffset, frameLength);
					termBuffer.PutLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
				}

				FrameLengthOrdered(termBuffer, termOffset, frameLength);
			}

			return position;
		}

		private long AppendFragmentedMessage(UnsafeBuffer termBuffer, int tailCounterOffset,
			DirectBufferVector[] vectors, int length, ReservedValueSupplier reservedValueSupplier)
		{
			int framedLength = ComputeFragmentedFrameLength(length, MaxPayloadLength);
			int termLength = termBuffer.Capacity;

			long rawTail = _logMetaDataBuffer.GetAndAddLong(tailCounterOffset, framedLength);
			int termId = TermId(rawTail);
			int termOffset = TermOffset(rawTail, termLength);

			int resultingOffset = termOffset + framedLength;
			long position = ComputePosition(termId, resultingOffset, PositionBitsToShift, InitialTermId);
			if (resultingOffset > termLength)
			{
				return HandleEndOfLog(termBuffer, termLength, termId, termOffset, position);
			}
			else
			{
				int frameOffset = termOffset;
				byte flags = BEGIN_FRAG_FLAG;
				int remaining = length;
				int vectorIndex = 0;
				int vectorOffset = 0;

				do
				{
					int bytesToWrite = Math.Min(remaining, MaxPayloadLength);
					int frameLength = bytesToWrite + HEADER_LENGTH;
					int alignedLength = Align(frameLength, FRAME_ALIGNMENT);

					_headerWriter.Write(termBuffer, frameOffset, frameLength, termId);

					int bytesWritten = 0;
					int payloadOffset = frameOffset + HEADER_LENGTH;
					do
					{
						DirectBufferVector vector = vectors[vectorIndex];
						int vectorRemaining = vector.Length() - vectorOffset;
						int numBytes = Math.Min(bytesToWrite - bytesWritten, vectorRemaining);

						termBuffer.PutBytes(payloadOffset, vector.Buffer(), vector.Offset() + vectorOffset, numBytes);

						bytesWritten += numBytes;
						payloadOffset += numBytes;
						vectorOffset += numBytes;

						if (vectorRemaining <= numBytes)
						{
							vectorIndex++;
							vectorOffset = 0;
						}
					} while (bytesWritten < bytesToWrite);

					if (remaining <= MaxPayloadLength)
					{
						flags |= END_FRAG_FLAG;
					}

					FrameFlags(termBuffer, frameOffset, flags);

					if (null != reservedValueSupplier)
					{
						long reservedValue = reservedValueSupplier(termBuffer, frameOffset, frameLength);
						termBuffer.PutLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
					}

					FrameLengthOrdered(termBuffer, frameOffset, frameLength);

					flags = 0;
					frameOffset += alignedLength;
					remaining -= bytesToWrite;
				} while (remaining > 0);
			}

			return position;
		}

		private long Claim(UnsafeBuffer termBuffer, int tailCounterOffset, int length, BufferClaim bufferClaim)
		{
			int frameLength = length + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			long rawTail = _logMetaDataBuffer.GetAndAddLong(tailCounterOffset, alignedLength);
			int termId = TermId(rawTail);
			int termOffset = TermOffset(rawTail, termLength);

			int resultingOffset = termOffset + alignedLength;
			long position = ComputePosition(termId, resultingOffset, PositionBitsToShift, InitialTermId);
			if (resultingOffset > termLength)
			{
				return HandleEndOfLog(termBuffer, termLength, termId, termOffset, position);
			}
			else
			{
				_headerWriter.Write(termBuffer, termOffset, frameLength, termId);
				bufferClaim.Wrap(termBuffer, termOffset, frameLength);
			}

			return position;
		}

		private long HandleEndOfLog(UnsafeBuffer termBuffer, int termLength, int termId, int termOffset, long position)
		{
			if (termOffset < termLength)
			{
				int paddingLength = termLength - termOffset;
				_headerWriter.Write(termBuffer, termOffset, paddingLength, termId);
				FrameType(termBuffer, termOffset, PADDING_FRAME_TYPE);
				FrameLengthOrdered(termBuffer, termOffset, paddingLength);
			}

			if (position >= MaxPossiblePosition)
			{
				return MAX_POSITION_EXCEEDED;
			}

			RotateLog(_logMetaDataBuffer, termId - InitialTermId, termId);

			return ADMIN_ACTION;
		}
	}
}