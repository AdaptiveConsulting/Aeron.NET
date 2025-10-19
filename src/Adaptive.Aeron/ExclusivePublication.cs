/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Runtime.CompilerServices;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;
using static Adaptive.Aeron.LogBuffer.LogBufferDescriptor;
using static Adaptive.Aeron.Protocol.DataHeaderFlyweight;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron
{
	/// <summary>
	/// Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. ExclusivePublications
	/// each get their own session id so multiple can be concurrently active on the same media driver as independent streams.
	/// <para>
	/// <seealso cref="ExclusivePublication"/>s are created via the <seealso cref="Aeron.AddExclusivePublication(String, int)"/> method,
	/// and messages are sent via one of the <seealso cref="Publication.Offer(UnsafeBuffer)"/> methods, or a
	/// <seealso cref="TryClaim(int, BufferClaim)"/> and <seealso cref="BufferClaim.Commit()"/> method combination.
	/// </para>
	/// <para>
	/// <seealso cref="ExclusivePublication"/>s have the potential to provide greater throughput than the default <seealso cref="Publication"/>
	/// which supports concurrent access.
	/// </para>
	/// <para>
	/// The APIs for tryClaim and offer are non-blocking.
	/// </para>
	/// <para>
	/// <b>Note:</b> Instances are NOT threadsafe for offer and tryClaim methods but are for the others.
	/// 
	/// </para>
	/// </summary>
	/// <seealso cref="Aeron.AddExclusivePublication(String, int)"></seealso>
	public sealed class ExclusivePublication : Publication
	{
		private long _termBeginPosition;
		private int _activePartitionIndex;
		private int _termId;
		private int _termOffset;

		// For testing purposes only
		internal ExclusivePublication()
		{
			
		}
		
		internal ExclusivePublication(
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
			var logMetaDataBuffer = logBuffers.MetaDataBuffer();
			var termCount = ActiveTermCount(logMetaDataBuffer);
			var index = IndexByTermCount(termCount);
			_activePartitionIndex = index;

			var rawTail = RawTail(base._logMetaDataBuffer, index);
			_termId = LogBufferDescriptor.TermId(rawTail);
			_termOffset = LogBufferDescriptor.TermOffset(rawTail);
			_termBeginPosition = ComputeTermBeginPosition(_termId, PositionBitsToShift, InitialTermId);
		}

		/// <summary>
		/// Mark the publication to be revoked when <seealso cref="Publication.Dispose()"/> is called.  See  <seealso cref="Revoke()"/>
		/// </summary>
		public void RevokeOnClose()
		{
			revokeOnClose = true;
		}

		/// <summary>
		/// Immediately revoke and <seealso cref="Publication.Dispose()"/> the publication.
		/// 
		/// Revoking disposes of resources as soon as possible. On the publication side the log buffer won't linger,
		/// while on the subscription side the image will go unavailable without requiring all data to be drained.
		/// Hence, it should be used only when it's known that all subscribers have received all the data,
		/// or if it doesn't matter if they have.
		/// </summary>
		public void Revoke()
		{
			if (!_isClosed)
			{
				revokeOnClose = true;
				Dispose();
			}
		}
		
		/// <inheritdoc />
		public override long Position
		{
			get
			{
				if (_isClosed)
				{
					return CLOSED;
				}

				return _termBeginPosition + _termOffset;
			}
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

				return _positionLimit.GetVolatile() - (_termBeginPosition + _termOffset);
			}
		}

		/// <summary>
		/// The current term-id of the publication.
		/// </summary>
		/// <returns> the current term-id of the publication. </returns>
		public int TermId()
		{
			return _termId;
		}

		/// <summary>
		/// The current term-offset of the publication.
		/// </summary>
		/// <returns> the current term-offset of the publication. </returns>
		public int TermOffset()
		{
			return _termOffset;
		}

		/// <inheritdoc />
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override long Offer(
			IDirectBuffer buffer,
			int offset,
			int length,
			ReservedValueSupplier reservedValueSupplier = null)
		{
			var newPosition = CLOSED;
			if (!_isClosed)
			{
				var limit = _positionLimit.GetVolatile();
				long position = _termBeginPosition + _termOffset;

				if (position < limit)
				{
					int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (_activePartitionIndex * SIZE_OF_LONG);
					UnsafeBuffer termBuffer = _termBuffers[_activePartitionIndex];
					int result;

					if (length <= MaxPayloadLength)
					{
						CheckPositiveLength(length);
						result = AppendUnfragmentedMessage(
							tailCounterOffset, termBuffer, buffer, offset, length, reservedValueSupplier);
					}
					else
					{
						CheckMaxMessageLength(length);
						result = AppendFragmentedMessage(
							termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
					}

					newPosition = NewPosition(result);
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
				long position = _termBeginPosition + _termOffset;
				int length = ValidateAndComputeLength(lengthOne, lengthTwo);

				if (position < limit)
				{
					int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (_activePartitionIndex * SIZE_OF_LONG);
					UnsafeBuffer termBuffer = _termBuffers[_activePartitionIndex];
					int result;

					if (length <= MaxPayloadLength)
					{
						CheckPositiveLength(length);
						result = AppendUnfragmentedMessage(
							termBuffer,
							tailCounterOffset,
							bufferOne, offsetOne, lengthOne,
							bufferTwo, offsetTwo, lengthTwo,
							reservedValueSupplier);
					}
					else
					{
						CheckMaxMessageLength(length);
						result = AppendFragmentedMessage(
							termBuffer,
							tailCounterOffset,
							bufferOne, offsetOne, lengthOne,
							bufferTwo, offsetTwo, lengthTwo,
							MaxPayloadLength,
							reservedValueSupplier);
					}

					newPosition = NewPosition(result);
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
			var newPosition = CLOSED;
			if (!_isClosed)
			{
				var limit = _positionLimit.GetVolatile();
				long position = _termBeginPosition + _termOffset;

				if (position < limit)
				{
					int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (_activePartitionIndex * SIZE_OF_LONG);
					UnsafeBuffer termBuffer = _termBuffers[_activePartitionIndex];
					int result;

					if (length <= MaxPayloadLength)
					{
						result = AppendUnfragmentedMessage(
							termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
					}
					else
					{
						CheckMaxMessageLength(length);
						result = AppendFragmentedMessage(
							termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
					}

					newPosition = NewPosition(result);
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}

		/// <inheritdoc />
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override long TryClaim(int length, BufferClaim bufferClaim)
		{
			CheckPayloadLength(length);
			var newPosition = CLOSED;

			if (!_isClosed)
			{
				var limit = _positionLimit.GetVolatile();
				long position = _termBeginPosition + _termOffset;

				if (position < limit)
				{
					int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (_activePartitionIndex * SIZE_OF_LONG);
					UnsafeBuffer termBuffer = _termBuffers[_activePartitionIndex];
					int result = Claim(termBuffer, tailCounterOffset, length, bufferClaim);

					newPosition = NewPosition(result);
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}

		/// <summary>
		/// Append a padding record to log of a given length to make up the log to a position.
		/// </summary>
		/// <param name="length"> of the range to claim, in bytes.. </param>
		/// <returns> The new stream position, otherwise a negative error value of <seealso cref="Publication.NOT_CONNECTED"/>,
		/// <seealso cref="Publication.BACK_PRESSURED"/>, <seealso cref="Publication.ADMIN_ACTION"/>, <seealso cref="Publication.CLOSED"/>, or <seealso cref="Publication.MAX_POSITION_EXCEEDED"/>. </returns>
		/// <exception cref="ArgumentException"> if the length is greater than <seealso cref="Publication.MaxMessageLength"/> framed. </exception>
		public long AppendPadding(int length)
		{
			if (length > _maxFramedLength)
			{
				ThrowHelper.ThrowArgumentException(
					$"message exceeds maxFramedLength of {_maxFramedLength:D}, length={length:D}");
			}

			long newPosition = CLOSED;
			if (!_isClosed)
			{
				long limit = _positionLimit.GetVolatile();
				long position = _termBeginPosition + _termOffset;

				if (position < limit)
				{
					CheckPositiveLength(length);
					int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (_activePartitionIndex * SIZE_OF_LONG);
					UnsafeBuffer termBuffer = _termBuffers[_activePartitionIndex];
					int result = AppendPadding(termBuffer, tailCounterOffset, length);

					newPosition = NewPosition(result);
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}

		/// <summary>
		/// Offer a block of pre-formatted message fragments directly into the current term.
		/// </summary>
		/// <param name="buffer"> containing the pre-formatted block of message fragments. </param>
		/// <param name="offset"> offset in the buffer at which the first fragment begins. </param>
		/// <param name="length"> in bytes of the encoded block. </param>
		/// <returns> The new stream position, otherwise a negative error value of <seealso cref="Publication.NOT_CONNECTED"/>,
		/// <seealso cref="Publication.BACK_PRESSURED"/>, <seealso cref="Publication.ADMIN_ACTION"/>, <seealso cref="Publication.CLOSED"/>,
		/// or <seealso cref="Publication.MAX_POSITION_EXCEEDED"/>. </returns>
		/// <exception cref="ArgumentException"> if the length is greater than remaining size of the current term. </exception>
		/// <exception cref="ArgumentException"> if the first frame within the block is not properly formatted, i.e. if the
		/// <code>streamId</code> is not equal to the value returned by the <seealso cref="Publication.StreamId"/>
		/// method or if the <code>sessionId</code> is not equal to the value returned by the
		/// <seealso cref="Publication.SessionId"/> method or if the frame type is not equal to the
		/// <seealso cref="HeaderFlyweight.HDR_TYPE_DATA"/>. </exception>
		public long OfferBlock(IMutableDirectBuffer buffer, int offset, int length)
		{
			if (IsClosed)
			{
				return CLOSED;
			}

			if (_termOffset >= TermBufferLength)
			{
				RotateTerm();
			}

			long limit = _positionLimit.GetVolatile();
			long position = _termBeginPosition + _termOffset;

			if (position < limit)
			{
				CheckBlockLength(length);
				CheckFirstFrame(buffer, offset);

				int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (_activePartitionIndex * SIZE_OF_LONG);
				UnsafeBuffer termBuffer = _termBuffers[_activePartitionIndex];
				int result = AppendBlock(termBuffer, tailCounterOffset, buffer, offset, length);

				return NewPosition(result);
			}
			else
			{
				return BackPressureStatus(position, length);
			}
		}

		private void CheckBlockLength(int length)
		{
			int remaining = TermBufferLength - _termOffset;
			if (length > remaining)
			{
				throw new ArgumentException("invalid block length " + length + ", remaining space in term is " +
				                            remaining);
			}
		}

		private void CheckFirstFrame(IMutableDirectBuffer buffer, int offset)
		{
			int frameType = HeaderFlyweight.HDR_TYPE_DATA;
			int blockTermOffset = buffer.GetInt(offset + TERM_OFFSET_FIELD_OFFSET,
				ByteOrder.LittleEndian);
			int blockSessionId =
				buffer.GetInt(offset + SESSION_ID_FIELD_OFFSET, ByteOrder.LittleEndian);
			int blockStreamId =
				buffer.GetInt(offset + STREAM_ID_FIELD_OFFSET, ByteOrder.LittleEndian);
			int blockTermId = buffer.GetInt(offset + TERM_ID_FIELD_OFFSET, ByteOrder.LittleEndian);
			int blockFrameType = buffer.GetShort(offset + HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LittleEndian) &
			                     0xFFFF;

			if (blockTermOffset != _termOffset || blockSessionId != SessionId || blockStreamId != StreamId ||
			    blockTermId != _termId || frameType != blockFrameType)
			{
				throw new ArgumentException("improperly formatted block:" + " termOffset=" + blockTermOffset +
				                            " (expected=" + _termOffset + ")," + " sessionId=" + blockSessionId +
				                            " (expected=" + SessionId + ")," + " streamId=" + blockStreamId +
				                            " (expected=" + StreamId + ")," + " termId=" + blockTermId +
				                            " (expected=" + _termId + ")," + " frameType=" + blockFrameType +
				                            " (expected=" + frameType + ")");
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private long NewPosition(int resultingOffset)
		{
			if (resultingOffset > 0)
			{
				_termOffset = resultingOffset;

				return _termBeginPosition + resultingOffset;
			}

			if ((_termBeginPosition + TermBufferLength) >= MaxPossiblePosition)
			{
				return MAX_POSITION_EXCEEDED;
			}

			RotateTerm();

			return ADMIN_ACTION;
		}

		private void RotateTerm()
		{
			int nextIndex = NextPartitionIndex(_activePartitionIndex);
			int nextTermId = _termId + 1;

			_activePartitionIndex = nextIndex;
			_termOffset = 0;
			_termId = nextTermId;
			_termBeginPosition += TermBufferLength;

			var termCount = nextTermId - InitialTermId;

			InitialiseTailWithTermId(_logMetaDataBuffer, nextIndex, nextTermId);
			ActiveTermCountOrdered(_logMetaDataBuffer, termCount);
		}

		private int HandleEndOfLog(UnsafeBuffer termBuffer, int termLength)
		{
			if (_termOffset < termLength)
			{
				int offset = _termOffset;
				int paddingLength = termLength - offset;
				_headerWriter.Write(termBuffer, offset, paddingLength, _termId);
				FrameType(termBuffer, offset, PADDING_FRAME_TYPE);
				FrameLengthOrdered(termBuffer, offset, paddingLength);
			}

			return -1;
		}

		private int AppendUnfragmentedMessage(
			int tailCounterOffset,
			UnsafeBuffer termBuffer, 
			IDirectBuffer srcBuffer,
			int srcOffset, 
			int length,
			ReservedValueSupplier reservedValueSupplier)
		{
			int frameLength = length + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + alignedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				_headerWriter.Write(termBuffer, _termOffset, frameLength, _termId);
				termBuffer.PutBytes(_termOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);

				if (null != reservedValueSupplier)
				{
					long reservedValue = reservedValueSupplier(termBuffer, _termOffset, frameLength);
					termBuffer.PutLong(_termOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
				}

				FrameLengthOrdered(termBuffer, _termOffset, frameLength);
			}

			return resultingOffset;
		}

		private int AppendFragmentedMessage(
			UnsafeBuffer termBuffer, 
			int tailCounterOffset, 
			IDirectBuffer srcBuffer,
			int srcOffset, 
			int length,
			ReservedValueSupplier reservedValueSupplier)
		{
			int framedLength = ComputeFragmentedFrameLength(length, MaxPayloadLength);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + framedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				int frameOffset = _termOffset;
				byte flags = BEGIN_FRAG_FLAG;
				int remaining = length;
				do
				{
					int bytesToWrite = Math.Min(remaining, MaxPayloadLength);
					int frameLength = bytesToWrite + HEADER_LENGTH;
					int alignedLength = Align(frameLength, FRAME_ALIGNMENT);

					_headerWriter.Write(termBuffer, frameOffset, frameLength, _termId);
					termBuffer.PutBytes(frameOffset + HEADER_LENGTH, srcBuffer, srcOffset + (length - remaining),
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

			return resultingOffset;
		}

		private int AppendUnfragmentedMessage(
			UnsafeBuffer termBuffer,
			int tailCounterOffset, 
			IDirectBuffer bufferOne,
			int offsetOne,
			int lengthOne, 
			IDirectBuffer bufferTwo, 
			int offsetTwo, 
			int lengthTwo,
			ReservedValueSupplier reservedValueSupplier)
		{
			int frameLength = lengthOne + lengthTwo + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + alignedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				_headerWriter.Write(termBuffer, _termOffset, frameLength, _termId);
				termBuffer.PutBytes(_termOffset + HEADER_LENGTH, bufferOne, offsetOne, lengthOne);
				termBuffer.PutBytes(_termOffset + HEADER_LENGTH + lengthOne, bufferTwo, offsetTwo, lengthTwo);

				if (null != reservedValueSupplier)
				{
					long reservedValue = reservedValueSupplier(termBuffer, _termOffset, frameLength);
					termBuffer.PutLong(_termOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
				}

				FrameLengthOrdered(termBuffer, _termOffset, frameLength);
			}

			return resultingOffset;
		}

		private int AppendFragmentedMessage(
			UnsafeBuffer termBuffer, 
			int tailCounterOffset, 
			IDirectBuffer bufferOne,
			int offsetOne, 
			int lengthOne, 
			IDirectBuffer bufferTwo,
			int offsetTwo,
			int lengthTwo, 
			int maxPayloadLength,
			ReservedValueSupplier reservedValueSupplier)
		{
			int length = lengthOne + lengthTwo;
			int framedLength = ComputeFragmentedFrameLength(length, MaxPayloadLength);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + framedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				int frameOffset = _termOffset;
				byte flags = BEGIN_FRAG_FLAG;
				int remaining = length;
				int positionOne = 0;
				int positionTwo = 0;

				do
				{
					int bytesToWrite = Math.Min(remaining, maxPayloadLength);
					int frameLength = bytesToWrite + HEADER_LENGTH;
					int alignedLength = Align(frameLength, FRAME_ALIGNMENT);

					_headerWriter.Write(termBuffer, frameOffset, frameLength, _termId);

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

					if (remaining <= maxPayloadLength)
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

			return resultingOffset;
		}

		private int AppendUnfragmentedMessage(
			UnsafeBuffer termBuffer, 
			int tailCounterOffset,
			DirectBufferVector[] vectors, 
			int length, 
			ReservedValueSupplier reservedValueSupplier)
		{
			int frameLength = length + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + alignedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				_headerWriter.Write(termBuffer, _termOffset, frameLength, _termId);

				int offset = _termOffset + HEADER_LENGTH;
				foreach (DirectBufferVector vector in vectors)
				{
					termBuffer.PutBytes(offset, vector.Buffer(), vector.Offset(), vector.Length());
					offset += vector.Length();
				}

				if (null != reservedValueSupplier)
				{
					long reservedValue = reservedValueSupplier(termBuffer, _termOffset, frameLength);
					termBuffer.PutLong(_termOffset + RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
				}

				FrameLengthOrdered(termBuffer, _termOffset, frameLength);
			}

			return resultingOffset;
		}

		private int AppendFragmentedMessage(
			UnsafeBuffer termBuffer, 
			int tailCounterOffset,
			DirectBufferVector[] vectors, 
			int length, 
			ReservedValueSupplier reservedValueSupplier)
		{
			int framedLength = ComputeFragmentedFrameLength(length, MaxPayloadLength);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + framedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				int frameOffset = _termOffset;
				byte flags = BEGIN_FRAG_FLAG;
				int remaining = length;
				int vectorIndex = 0;
				int vectorOffset = 0;

				do
				{
					int bytesToWrite = Math.Min(remaining, MaxPayloadLength);
					int frameLength = bytesToWrite + HEADER_LENGTH;
					int alignedLength = Align(frameLength, FRAME_ALIGNMENT);

					_headerWriter.Write(termBuffer, frameOffset, frameLength, _termId);

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

			return resultingOffset;
		}

		private int Claim(
			UnsafeBuffer termBuffer, 
			int tailCounterOffset, 
			int length, 
			BufferClaim bufferClaim)
		{
			int frameLength = length + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + alignedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				_headerWriter.Write(termBuffer, _termOffset, frameLength, _termId);
				bufferClaim.Wrap(termBuffer, _termOffset, frameLength);
			}

			return resultingOffset;
		}

		private int AppendPadding(
			UnsafeBuffer termBuffer, 
			int tailCounterOffset, 
			int length)
		{
			int frameLength = length + HEADER_LENGTH;
			int alignedLength = Align(frameLength, FRAME_ALIGNMENT);
			int termLength = termBuffer.Capacity;

			int resultingOffset = _termOffset + alignedLength;
			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));

			if (resultingOffset > termLength)
			{
				resultingOffset = HandleEndOfLog(termBuffer, termLength);
			}
			else
			{
				_headerWriter.Write(termBuffer, _termOffset, frameLength, _termId);
				FrameType(termBuffer, _termOffset, PADDING_FRAME_TYPE);
				FrameLengthOrdered(termBuffer, _termOffset, frameLength);
			}

			return resultingOffset;
		}

		private int AppendBlock(
			UnsafeBuffer termBuffer, 
			int tailCounterOffset,
			IMutableDirectBuffer buffer, 
			int offset,
			int length)
		{
			int resultingOffset = _termOffset + length;
			int lengthOfFirstFrame = buffer.GetInt(offset, ByteOrder.LittleEndian);

			_logMetaDataBuffer.PutLongRelease(tailCounterOffset, PackTail(_termId, resultingOffset));
			
			termBuffer.PutBytes(_termOffset + HEADER_LENGTH, buffer, offset + HEADER_LENGTH, length - HEADER_LENGTH);
			termBuffer.PutLong(_termOffset + 24, buffer.GetLong(offset + 24));
			termBuffer.PutLong(_termOffset + 16, buffer.GetLong(offset + 16));
			termBuffer.PutLong(_termOffset + 8, buffer.GetLong(offset + 8));
			termBuffer.PutLongRelease(_termOffset, buffer.GetLong(offset));

			return resultingOffset;
		}
	}
}