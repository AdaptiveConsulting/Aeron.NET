using System.Runtime.CompilerServices;

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Reports a position by recording it in an <seealso cref="UnsafeBuffer"/>.
    /// </summary>
    public class UnsafeBufferPosition : IPosition
    {
        private readonly int _counterId;
        private readonly int _offset;
        private readonly UnsafeBuffer _buffer;
        private readonly CountersManager _countersManager;

        /// <summary>
        /// Map a position over a buffer.
        /// </summary>
        /// <param name="buffer">    containing the counter. </param>
        /// <param name="counterId"> identifier of the counter. </param>
        public UnsafeBufferPosition(UnsafeBuffer buffer, int counterId) : this(buffer, counterId, null)
        {
        }

        /// <summary>
        /// Map a position over a buffer and this indicator owns the counter for reclamation.
        /// </summary>
        /// <param name="buffer">          containing the counter. </param>
        /// <param name="counterId">       identifier of the counter. </param>
        /// <param name="countersManager"> to be used for freeing the counter when this is closed. </param>
        public UnsafeBufferPosition(UnsafeBuffer buffer, int counterId, CountersManager countersManager)
        {
            _buffer = buffer;
            _counterId = counterId;
            _countersManager = countersManager;
            _offset = CountersReader.CounterOffset(counterId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Id()
        {
            return _counterId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Get()
        {
            return _buffer.GetLong(_offset);
        }

        public long Volatile => _buffer.GetLongVolatile(_offset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long value)
        {
            _buffer.PutLong(_offset, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetOrdered(long value)
        {
            _buffer.PutLongOrdered(_offset, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ProposeMax(long proposedValue)
        {
            var buffer = _buffer;
            var offset = _offset;
            var updated = false;

            if (buffer.GetLong(offset) < proposedValue)
            {
                buffer.PutLong(offset, proposedValue);
                updated = true;
            }

            return updated;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ProposeMaxOrdered(long proposedValue)
        {
            var buffer = _buffer;
            var offset = _offset;
            var updated = false;

            if (buffer.GetLong(offset) < proposedValue)
            {
                buffer.PutLongOrdered(offset, proposedValue);
                updated = true;
            }

            return updated;
        }

        public void Dispose()
        {
            _countersManager?.Free(_counterId);
        }
    }
}