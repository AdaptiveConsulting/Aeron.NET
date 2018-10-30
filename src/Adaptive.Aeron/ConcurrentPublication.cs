using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
	/// Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. <seealso cref="Publication"/>s
	/// are created via the <seealso cref="Aeron.AddPublication(string, int)"/> method, and messages are sent via one of the
	/// <seealso cref="Publication.Offer(UnsafeBuffer)"/> methods, or a <seealso cref="TryClaim(int, BufferClaim)"/> and <seealso cref="BufferClaim.Commit()"/>
	/// method combination.
	/// <para>
	/// The APIs used for try claim and offer are non-blocking and thread safe.
	/// </para>
	/// <para>
	/// <b>Note:</b> Instances are threadsafe and can be shared between publishing threads.
	/// 
	/// </para>
	/// </summary>
	/// <seealso cref="Aeron.AddPublication(string, int)"></seealso>
	/// <seealso cref="BufferClaim"></seealso>
	public class ConcurrentPublication : Publication
	{
		private readonly TermAppender[] _termAppenders = new TermAppender[LogBufferDescriptor.PARTITION_COUNT];

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
			var buffers = logBuffers.DuplicateTermBuffers();
			
			for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
			{
				_termAppenders[i] = new TermAppender(buffers[i], _logMetaDataBuffer, i);
			}
		}

		/// <inheritdoc />
		public override long Offer(IDirectBuffer buffer, int offset, int length, ReservedValueSupplier reservedValueSupplier = null)
		{
			long newPosition = CLOSED;
			if (!_isClosed)
			{
				long limit = _positionLimit.GetVolatile();
				int termCount = LogBufferDescriptor.ActiveTermCount(_logMetaDataBuffer);
				TermAppender termAppender = _termAppenders[LogBufferDescriptor.IndexByTermCount(termCount)];
				long rawTail = termAppender.RawTailVolatile();
				long termOffset = rawTail & 0xFFFF_FFFFL;
				int termId = LogBufferDescriptor.TermId(rawTail);
				long position = LogBufferDescriptor.ComputeTermBeginPosition(termId, _positionBitsToShift, InitialTermId) + termOffset;

				if (termCount != (termId - InitialTermId))
				{
					return ADMIN_ACTION;
				}

				if (position < limit)
				{
					int resultingOffset;
					if (length <= MaxPayloadLength)
					{
						resultingOffset = termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, offset, length, reservedValueSupplier, termId);
					}
					else
					{
						CheckForMaxMessageLength(length);
						resultingOffset = termAppender.AppendFragmentedMessage(_headerWriter, buffer, offset, length, MaxPayloadLength, reservedValueSupplier, termId);
					}

					newPosition = NewPosition(termCount, (int)termOffset, termId, position, resultingOffset);
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
				int termCount = LogBufferDescriptor.ActiveTermCount(_logMetaDataBuffer);
				TermAppender termAppender = _termAppenders[LogBufferDescriptor.IndexByTermCount(termCount)];
				long rawTail = termAppender.RawTailVolatile();
				long termOffset = rawTail & 0xFFFF_FFFFL;
				int termId = LogBufferDescriptor.TermId(rawTail);
				long position = LogBufferDescriptor.ComputeTermBeginPosition(termId, _positionBitsToShift, InitialTermId) + termOffset;

				if (termCount != (termId - InitialTermId))
				{
					return ADMIN_ACTION;
				}

				if (position < limit)
				{
					int resultingOffset;
					if (length <= MaxPayloadLength)
					{
						resultingOffset = termAppender.AppendUnfragmentedMessage(_headerWriter, vectors, length, reservedValueSupplier, termId);
					}
					else
					{
						CheckForMaxMessageLength(length);
						resultingOffset = termAppender.AppendFragmentedMessage(_headerWriter, vectors, length, MaxPayloadLength, reservedValueSupplier, termId);
					}

					newPosition = NewPosition(termCount, (int)termOffset, termId, position, resultingOffset);
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
			CheckForMaxPayloadLength(length);
			long newPosition = CLOSED;

			if (!_isClosed)
			{
				long limit = _positionLimit.GetVolatile();
				int termCount = LogBufferDescriptor.ActiveTermCount(_logMetaDataBuffer);
				TermAppender termAppender = _termAppenders[LogBufferDescriptor.IndexByTermCount(termCount)];
				long rawTail = termAppender.RawTailVolatile();
				long termOffset = rawTail & 0xFFFF_FFFFL;
				int termId = LogBufferDescriptor.TermId(rawTail);
				long position = LogBufferDescriptor.ComputeTermBeginPosition(termId, _positionBitsToShift, InitialTermId) + termOffset;

				if (termCount != (termId - InitialTermId))
				{
					return ADMIN_ACTION;
				}

				if (position < limit)
				{
					int resultingOffset = termAppender.Claim(_headerWriter, length, bufferClaim, termId);
					newPosition = NewPosition(termCount, (int)termOffset, termId, position, resultingOffset);
				}
				else
				{
					newPosition = BackPressureStatus(position, length);
				}
			}

			return newPosition;
		}

		private long NewPosition(int termCount, int termOffset, int termId, long position, int resultingOffset)
		{
			if (resultingOffset > 0)
			{
				return (position - termOffset) + resultingOffset;
			}

			if ((position + termOffset) > _maxPossiblePosition)
			{
				return MAX_POSITION_EXCEEDED;
			}

			LogBufferDescriptor.RotateLog(_logMetaDataBuffer, termCount, termId);

			return ADMIN_ACTION;
		}
	}

}