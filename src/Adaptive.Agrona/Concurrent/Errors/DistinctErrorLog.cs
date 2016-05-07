using System;
using System.Text;

namespace Adaptive.Agrona.Concurrent.Errors
{
    /// <summary>
    /// Distinct record of error observations. Rather than grow a record indefinitely when many errors of the same type
    /// are logged, this log takes the approach of only recording distinct errors of the same type type and stack trace
    /// and keeping a count and time of observation so that the record only grows with new distinct observations.
    /// 
    /// The provided <seealso cref="IAtomicBuffer"/> can wrap a memory-mapped file so logging can be out of process. This provides
    /// the benefit that if a crash or lockup occurs then the log can be read externally without loss of data.
    /// 
    /// This class is threadsafe to be used from multiple logging threads.
    /// 
    /// The error records are recorded to the memory mapped buffer in the following format.
    /// 
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |R|                         Length                              |
    ///  +-+-------------------------------------------------------------+
    ///  |R|                     Observation Count                       |
    ///  +-+-------------------------------------------------------------+
    ///  |R|                Last Observation Timestamp                   |
    ///  |                                                               |
    ///  +-+-------------------------------------------------------------+
    ///  |R|               First Observation Timestamp                   |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                     UTF-8 Encoded Error                      ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public class DistinctErrorLog
    {
        /// <summary>
        /// Offset within a record at which the length field begins.
        /// </summary>
        public const int LengthOffset = 0;

        /// <summary>
        /// Offset within a record at which the observation count field begins.
        /// </summary>
        public static readonly int ObservationCountOffset = BitUtil.SizeOfInt;

        /// <summary>
        /// Offset within a record at which the last observation timestamp field begins.
        /// </summary>
        public static readonly int LastObservationTimestampOffset = ObservationCountOffset + BitUtil.SizeOfInt;

        /// <summary>
        /// Offset within a record at which the first observation timestamp field begins.
        /// </summary>
        public static readonly int FirstObservationTimestampOffset = LastObservationTimestampOffset + BitUtil.SizeOfLong;

        /// <summary>
        /// Offset within a record at which the encoded exception field begins.
        /// </summary>
        public static readonly int EncodedErrorOffset = FirstObservationTimestampOffset + BitUtil.SizeOfLong;

        /// <summary>
        /// Alignment to be applied for record beginning.
        /// </summary>
        public static readonly int RecordAlignment = BitUtil.SizeOfLong;

        private static readonly DistinctObservation InsufficientSpace = new DistinctObservation(null, 0);

        private int _nextOffset = 0;
        private readonly IEpochClock _clock;
        private readonly IAtomicBuffer _buffer;
        private volatile DistinctObservation[] _distinctObservations = new DistinctObservation[0];
        private readonly object _newObservationLock = new object();

        /// <summary>
        /// Create a new error log that will be written to a provided <seealso cref="IAtomicBuffer"/>.
        /// </summary>
        /// <param name="buffer"> into which the observation records are recorded. </param>
        /// <param name="clock">  to be used for time stamping records. </param>
        public DistinctErrorLog(IAtomicBuffer buffer, IEpochClock clock)
        {
            buffer.VerifyAlignment();
            _clock = clock;
            _buffer = buffer;
        }

        /// <summary>
        /// Record an observation of an error. If it is the first observation of this error type for a stack trace
        /// then a new entry will be created. For subsequent observations of the same error type and stack trace a
        /// counter and time of last observation will be updated.
        /// </summary>
        /// <param name="observation"> to be logged as an error observation. </param>
        /// <returns> true if successfully logged otherwise false if insufficient space remaining in the log. </returns>
        public virtual bool Record(Exception observation)
        {
            long timestamp = _clock.Time();
            var existingObservations = _distinctObservations;
            var existingObservation = Find(existingObservations, observation);

            if (null == existingObservation)
            {
                lock (_newObservationLock)
                {
                    existingObservation = NewObservation(timestamp, existingObservations, observation);
                    if (InsufficientSpace == existingObservation)
                    {
                        return false;
                    }
                }
            }

            int offset = existingObservation.Offset;
            _buffer.GetAndAddInt(offset + ObservationCountOffset, 1);
            _buffer.PutLongOrdered(offset + LastObservationTimestampOffset, timestamp);

            return true;
        }

        private static DistinctObservation Find(DistinctObservation[] existingObservations, Exception observation)
        {
            DistinctObservation existingObservation = null;

            foreach (var o in existingObservations)
            {
                if (ExceptionEquals(o.Throwable, observation))
                {
                    existingObservation = o;
                    break;
                }
            }

            return existingObservation;
        }

        private static bool ExceptionEquals(Exception lhs, Exception rhs)
        {
            while (true)
            {
                if (lhs == rhs)
                {
                    return true;
                }

                if (lhs.Message != rhs.Message 
                    || lhs.GetType() != rhs.GetType()
                    || lhs.StackTrace != rhs.StackTrace)
                {
                    return false;
                }

                lhs = lhs.InnerException;
                rhs = rhs.InnerException;

                if (null == lhs && null == rhs)
                {
                    return true;
                }
                if (null != lhs && null != rhs)
                {
                    continue;
                }

                return false;
            }
        }

        private DistinctObservation NewObservation(long timestamp, DistinctObservation[] existingObservations, Exception observation)
        {
            DistinctObservation existingObservation = null;

            if (existingObservations != _distinctObservations)
            {
                existingObservation = Find(_distinctObservations, observation);
            }

            if (null == existingObservation)
            {
                byte[] encodedError = Encoding.UTF8.GetBytes(observation.ToString());

                int length = EncodedErrorOffset + encodedError.Length;
                int offset = _nextOffset;

                if ((offset + length) > _buffer.Capacity)
                {
                    return InsufficientSpace;
                }

                _buffer.PutBytes(offset + EncodedErrorOffset, encodedError);
                _buffer.PutLong(offset + FirstObservationTimestampOffset, timestamp);
                _nextOffset = BitUtil.Align(offset + length, RecordAlignment);

                existingObservation = new DistinctObservation(observation, offset);
                _distinctObservations = Prepend(_distinctObservations, existingObservation);

                _buffer.PutIntOrdered(offset + LengthOffset, length);
            }

            return existingObservation;
        }

        private static DistinctObservation[] Prepend(DistinctObservation[] observations, DistinctObservation observation)
        {
            int length = observations.Length;
            var newObservations = new DistinctObservation[length + 1];

            newObservations[0] = observation;
            Array.Copy(observations, 0, newObservations, 1, length);

            return newObservations;
        }

        public sealed class DistinctObservation
        {
            public readonly Exception Throwable;
            public readonly int Offset;

            public DistinctObservation(Exception throwable, int offset)
            {
                Throwable = throwable;
                Offset = offset;
            }
        }

    }
}