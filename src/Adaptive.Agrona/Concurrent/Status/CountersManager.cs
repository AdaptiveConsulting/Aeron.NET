using System;
using System.Collections.Generic;

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Manages the allocation and freeing of counters that are normally stored in a memory-mapped file.
    /// 
    /// This class in not threadsafe. Counters should be centrally managed.
    /// 
    /// <b>Values Buffer</b>
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                        Counter Value                          |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                     120 bytes of padding                     ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    ///  |                   Repeats to end of buffer                   ...
    ///  |                                                               |
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// 
    /// <b>Meta Data Buffer</b>
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                        Record State                           |
    ///  +---------------------------------------------------------------+
    ///  |                          Type Id                              |
    ///  +---------------------------------------------------------------+
    ///  |                      120 bytes for key                       ...
    /// ...                                                              |
    ///  +-+-------------------------------------------------------------+
    ///  |R|                      Label Length                           |
    ///  +-+-------------------------------------------------------------+
    ///  |                  124 bytes of Label in UTF-8                 ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    ///  |                   Repeats to end of buffer                   ...
    ///  |                                                               |
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public class CountersManager : CountersReader
    {
        /// <summary>
        /// Default type id of a counter when none is supplied.
        /// </summary>
        public const int DEFAULT_TYPE_ID = 0;

        /// <summary>
        /// Default function to set a key when none is supplied.
        /// </summary>
        public static readonly Action<IMutableDirectBuffer> DEFAULT_KEY_FUNC = ignore => { };

        private int _idHighWaterMark = -1;
        private readonly Queue<int> _freeList = new Queue<int>();

        /// <summary>
        /// Create a new counter buffer manager over two buffers.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the types, keys, and labels for the counters. </param>
        /// <param name="valuesBuffer">   containing the values of the counters themselves. </param>
        public CountersManager(IAtomicBuffer metaDataBuffer, IAtomicBuffer valuesBuffer) : base(metaDataBuffer, valuesBuffer)
        {
            valuesBuffer.VerifyAlignment();

            if (metaDataBuffer.Capacity < valuesBuffer.Capacity*2)
            {
                throw new ArgumentException("Meta data buffer not sufficiently large");
            }
        }

        /// <summary>
        /// Allocate a new counter with a given label.
        /// </summary>
        /// <param name="label"> to describe the counter. </param>
        /// <returns> the id allocated for the counter. </returns>
        public int Allocate(string label)
        {
            return Allocate(label, DEFAULT_TYPE_ID, DEFAULT_KEY_FUNC);
        }

        /// <summary>
        /// Allocate a new counter with a given label.
        /// 
        /// The key function will be called with a buffer with the exact length of available key space
        /// in the record for the user to store what they want for the key. No offset is required.
        /// </summary>
        /// <param name="label">   to describe the counter. </param>
        /// <param name="typeId">  for the type of counter. </param>
        /// <param name="keyFunc"> for setting the key value for the counter. </param>
        /// <returns> the id allocated for the counter. </returns>
        public int Allocate(string label, int typeId, Action<IMutableDirectBuffer> keyFunc)
        {
            var counterId = NextCounterId();
            if ((CounterOffset(counterId) + COUNTER_LENGTH) > ValuesBuffer_Renamed.Capacity)
            {
                throw new ArgumentException("Unable to allocated counter, values buffer is full");
            }

            var recordOffset = MetaDataOffset(counterId);
            if ((recordOffset + METADATA_LENGTH) > MetaDataBuffer_Renamed.Capacity)
            {
                throw new ArgumentException("Unable to allocate counter, labels buffer is full");
            }

            MetaDataBuffer_Renamed.PutInt(recordOffset + TYPE_ID_OFFSET, typeId);
            keyFunc(new UnsafeBuffer(MetaDataBuffer_Renamed, recordOffset + KEY_OFFSET, MAX_KEY_LENGTH));
            MetaDataBuffer_Renamed.PutStringUtf8(recordOffset + LABEL_OFFSET, label, MAX_LABEL_LENGTH);

            MetaDataBuffer_Renamed.PutIntOrdered(recordOffset, RECORD_ALLOCATED);

            return counterId;
        }

        /// <summary>
        /// Allocate a counter record and wrap it with a new <seealso cref="AtomicCounter"/> for use.
        /// </summary>
        /// <param name="label"> to describe the counter. </param>
        /// <returns> a newly allocated <seealso cref="AtomicCounter"/> </returns>
        public AtomicCounter NewCounter(string label)
        {
            return new AtomicCounter(ValuesBuffer_Renamed, Allocate(label), this);
        }

        /// <summary>
        /// Allocate a counter record and wrap it with a new <seealso cref="AtomicCounter"/> for use.
        /// </summary>
        /// <param name="label">   to describe the counter. </param>
        /// <param name="typeId">  for the type of counter. </param>
        /// <param name="keyFunc"> for setting the key value for the counter.
        /// </param>
        /// <returns> a newly allocated <seealso cref="AtomicCounter"/> </returns>
        public AtomicCounter NewCounter(string label, int typeId, Action<IMutableDirectBuffer> keyFunc)
        {
            return new AtomicCounter(ValuesBuffer_Renamed, Allocate(label, typeId, keyFunc), this);
        }

        /// <summary>
        /// Free the counter identified by counterId.
        /// </summary>
        /// <param name="counterId"> the counter to freed </param>
        public void Free(int counterId)
        {
            MetaDataBuffer_Renamed.PutIntOrdered(MetaDataOffset(counterId), RECORD_RECLAIMED);
            _freeList.Enqueue(counterId);
        }

        /// <summary>
        /// Set an <seealso cref="AtomicCounter"/> value based on counterId.
        /// </summary>
        /// <param name="counterId"> to be set. </param>
        /// <param name="value">     to set for the counter. </param>
        public void SetCounterValue(int counterId, long value)
        {
            ValuesBuffer_Renamed.PutLongOrdered(CounterOffset(counterId), value);
        }

        private int NextCounterId()
        {
            if (_freeList.Count == 0)
            {
                return ++_idHighWaterMark;
            }

            var counterId = _freeList.Dequeue();
            ValuesBuffer_Renamed.PutLongOrdered(CounterOffset(counterId), 0L);

            return counterId;
        }
    }
}