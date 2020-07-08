/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Text;
using Adaptive.Agrona.Collections;

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Reads the counters metadata and values buffers.
    /// 
    /// This class is threadsafe and can be used across threads.
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
    ///  |                   Free-for-reuse Deadline                     |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                      112 bytes for key                       ...
    /// ...                                                              |
    ///  +-+-------------------------------------------------------------+
    ///  |R|                      Label Length                           |
    ///  +-+-------------------------------------------------------------+
    ///  |                      380 bytes of Label                      ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    ///  |                   Repeats to end of buffer                   ...
    ///  |                                                               |
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public class CountersReader
    {
        /// <summary>
        /// Accept a metadata record.
        /// </summary>
        /// <param name="counterId"> of the counter.</param>
        /// <param name="typeId"> of the counter.</param>
        /// <param name="keyBuffer"> for the counter.</param>
        /// <param name="label"> for the counter.</param>
        public delegate void MetaData(int counterId, int typeId, IDirectBuffer keyBuffer, string label);
        
        /// <summary>
        /// Callback function for consuming basic counter details and value.
        /// </summary>
        /// <param name="value">     of the counter. </param>
        /// <param name="counterId"> of the counter </param>
        /// <param name="label">     for the counter. </param>
        public delegate void CounterConsumer(long value, int counterId, string label);

        /// <summary>
        /// Can be used to representing a null counter id when passed as a argument.
        /// </summary>
        public const int NULL_COUNTER_ID = -1;
        
        /// <summary>
        /// Record has not been used.
        /// </summary>
        public const int RECORD_UNUSED = 0;

        /// <summary>
        /// Record currently allocated for use.
        /// </summary>
        public const int RECORD_ALLOCATED = 1;

        /// <summary>
        /// Record was active and now has been reclaimed.
        /// </summary>
        public const int RECORD_RECLAIMED = -1;

        /// <summary>
        /// Deadline to indicate counter is not free to be reused.
        /// </summary>
        public static readonly long NOT_FREE_TO_REUSE = long.MaxValue;

        /// <summary>
        /// Offset in the record at which the type id field is stored.
        /// </summary>
        public static readonly int TYPE_ID_OFFSET = BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Offset in the record at which the deadline (in milliseconds) for when counter may be reused.
        /// </summary>
        public static readonly int FREE_FOR_REUSE_DEADLINE_OFFSET = TYPE_ID_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Offset in the record at which the key is stored.
        /// </summary>
        public static readonly int KEY_OFFSET = FREE_FOR_REUSE_DEADLINE_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// Offset in the record at which the label is stored.
        /// </summary>
        public static readonly int LABEL_OFFSET = BitUtil.CACHE_LINE_LENGTH * 2;

        /// <summary>
        /// Length of a counter label length including length prefix.
        /// </summary>
        public static readonly int FULL_LABEL_LENGTH = BitUtil.CACHE_LINE_LENGTH * 6;

        /// <summary>
        /// Maximum length of a label not including its length prefix.
        /// </summary>
        public static readonly int MAX_LABEL_LENGTH = FULL_LABEL_LENGTH - BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Maximum length a key can be.
        /// </summary>
        public static readonly int MAX_KEY_LENGTH = (BitUtil.CACHE_LINE_LENGTH * 2) - (BitUtil.SIZE_OF_INT * 2) - BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// Length of a meta data record in bytes.
        /// </summary>
        public static readonly int METADATA_LENGTH = LABEL_OFFSET + FULL_LABEL_LENGTH;

        /// <summary>
        /// Length of the space allocated to a counter that includes padding to avoid false sharing.
        /// </summary>
        public static readonly int COUNTER_LENGTH = BitUtil.CACHE_LINE_LENGTH * 2;

        /// <summary>
        /// Construct a reader over buffers containing the values and associated metadata.
        /// 
        /// Counter labels default to <see cref="Encoding.UTF8"/>
        /// 
        /// </summary>
        /// <param name="metaDataBuffer"> containing the counter metadata. </param>
        /// <param name="valuesBuffer">   containing the counter values. </param>
        public CountersReader(IAtomicBuffer metaDataBuffer, IAtomicBuffer valuesBuffer)
            : this(metaDataBuffer, valuesBuffer, Encoding.UTF8)
        {
        }

        /// <summary>
        /// Construct a reader over buffers containing the values and associated metadata.
        /// 
        /// </summary>
        /// <param name="metaDataBuffer"> containing the counter metadata. </param>
        /// <param name="valuesBuffer">   containing the counter values. </param>
        /// <param name="encoding"> for the label encoding</param>
        public CountersReader(IAtomicBuffer metaDataBuffer, IAtomicBuffer valuesBuffer, Encoding encoding)
        {
            MaxCounterId = valuesBuffer.Capacity / COUNTER_LENGTH;
            ValuesBuffer = valuesBuffer;
            MetaDataBuffer = metaDataBuffer;
            LabelCharset = encoding;
        }

        /// <summary>
        /// Get the maximum counter id which can be supported given the length of the values buffer.
        /// </summary>
        /// <returns> the maximum counter id which can be supported given the length of the values buffer. </returns>
        public int MaxCounterId { get; protected set; }
        
        /// <summary>
        /// Get the buffer containing the metadata for the counters.
        /// </summary>
        /// <returns> the buffer containing the metadata for the counters. </returns>
        public IAtomicBuffer MetaDataBuffer { get; }

        /// <summary>
        /// Get the buffer containing the values for the counters.
        /// </summary>
        /// <returns> the buffer containing the values for the counters. </returns>
        public IAtomicBuffer ValuesBuffer { get; }

        /// <summary>
        /// The <see cref="Encoding"/> used for the encoded label.
        /// </summary>
        /// <returns> the <see cref="Encoding"/> used for the encoded label.</returns>
        public Encoding LabelCharset { get; }

        /// <summary>
        /// The offset in the counter buffer for a given counterId.
        /// </summary>
        /// <param name="counterId"> for which the offset should be provided. </param>
        /// <returns> the offset in the counter buffer. </returns>
        public static int CounterOffset(int counterId)
        {
            return counterId * COUNTER_LENGTH;
        }

        /// <summary>
        /// The offset in the metadata buffer for a given id.
        /// </summary>
        /// <param name="counterId"> for the record. </param>
        /// <returns> the offset at which the metadata record begins. </returns>
        public static int MetaDataOffset(int counterId)
        {
            return counterId * METADATA_LENGTH;
        }

        /// <summary>
        /// Iterate over all labels in the label buffer.
        /// </summary>
        /// <param name="consumer"> function to be called for each label. </param>
        public void ForEach(IntObjConsumer<string> consumer)
        {
            var counterId = 0;

            for (int i = 0, capacity = MetaDataBuffer.Capacity; i < capacity; i += METADATA_LENGTH)
            {
                var recordStatus = MetaDataBuffer.GetIntVolatile(i);
                if (RECORD_ALLOCATED == recordStatus)
                {
                    var label = LabelValue(i);
                    consumer(counterId, label);
                }
                else if (RECORD_UNUSED == recordStatus)
                {
                    break;
                }

                counterId++;
            }
        }
        
        /// <summary>
        /// Iterate over the counters and provide the value and basic metadata.
        /// </summary>
        /// <param name="consumer"> for each allocated counter. </param>
        public void ForEach(CounterConsumer consumer)
        {
            int counterId = 0;

            for (int i = 0, capacity = MetaDataBuffer.Capacity; i < capacity; i += METADATA_LENGTH)
            {
                int recordStatus = MetaDataBuffer.GetIntVolatile(i);

                if (RECORD_ALLOCATED == recordStatus)
                {
                    consumer(ValuesBuffer.GetLongVolatile(CounterOffset(counterId)), counterId, LabelValue(i));
                }
                else if (RECORD_UNUSED == recordStatus)
                {
                    break;
                }

                counterId++;
            }
        }

        /// <summary>
        /// Iterate over all the metadata in the buffer.
        /// </summary>
        /// <param name="metaData"> function to be called for each metadata record. </param>
        public void ForEach(MetaData metaData)
        {
            var counterId = 0;

            for (int i = 0, capacity = MetaDataBuffer.Capacity; i < capacity; i += METADATA_LENGTH)
            {
                var recordStatus = MetaDataBuffer.GetIntVolatile(i);
                if (RECORD_ALLOCATED == recordStatus)
                {
                    var typeId = MetaDataBuffer.GetInt(i + TYPE_ID_OFFSET);
                    var label = LabelValue(i);
                    IDirectBuffer keyBuffer = new UnsafeBuffer(MetaDataBuffer, i + KEY_OFFSET, MAX_KEY_LENGTH);

                    metaData(counterId, typeId, keyBuffer, label);
                }
                else if (RECORD_UNUSED == recordStatus)
                {
                    break;
                }

                counterId++;
            }
        }

        /// <summary>
        /// Get the value for a given counter id as a volatile read.
        /// </summary>
        /// <param name="counterId"> to be read. </param>
        /// <returns> the current value of the counter. </returns>
        public long GetCounterValue(int counterId)
        {
            ValidateCounterId(counterId);
            
            return ValuesBuffer.GetLongVolatile(CounterOffset(counterId));
        }
        
        /// <summary>
        /// Get the state for a given counter id as a volatile read.
        /// </summary>
        /// <param name="counterId"> to be read. </param>
        /// <returns> the current state of the counter. </returns>
        /// <seealso cref="RECORD_UNUSED"></seealso>
        /// <seealso cref="RECORD_ALLOCATED"></seealso>
        /// <seealso cref="RECORD_RECLAIMED"></seealso>
        public int GetCounterState(int counterId)
        {
            ValidateCounterId(counterId);

            return MetaDataBuffer.GetIntVolatile(MetaDataOffset(counterId));
        }
        
        /// <summary>
        /// Get the type id for a given counter id.
        /// </summary>
        /// <param name="counterId"> to be read. </param>
        /// <returns> the type id for a given counter id. </returns>
        public int GetCounterTypeId(int counterId)
        {
            ValidateCounterId(counterId);

            return MetaDataBuffer.GetInt(MetaDataOffset(counterId) + TYPE_ID_OFFSET);
        }

        /// <summary>
        /// Get the deadline (in milliseconds) for when a given counter id may be reused.
        /// </summary>
        /// <param name="counterId"> to be read. </param>
        /// <returns> deadline (in milliseconds) for when a given counter id may be reused or <seealso cref="NOT_FREE_TO_REUSE"/> if
        /// currently in use. </returns>
        public long GetFreeForReuseDeadline(int counterId)
        {
            ValidateCounterId(counterId);

            return MetaDataBuffer.GetLongVolatile(MetaDataOffset(counterId) + FREE_FOR_REUSE_DEADLINE_OFFSET);
        }

        /// <summary>
        /// Get the label for a given counter id.
        /// </summary>
        /// <param name="counterId"> to be read. </param>
        /// <returns> the label for the given counter id. </returns>
        public string GetCounterLabel(int counterId)
        {
            ValidateCounterId(counterId);

            return LabelValue(MetaDataOffset(counterId));
        }

        private void ValidateCounterId(int counterId)
        {
            if (counterId < 0 || counterId > MaxCounterId)
            {
                throw new System.ArgumentException("Counter id " + counterId + " out of range: maxCounterId=" + MaxCounterId);
            }
        }

        private string LabelValue(int recordOffset)
        {
            int labelLength = MetaDataBuffer.GetInt(recordOffset + LABEL_OFFSET);
            byte[] stringInBytes = new byte[labelLength];
            MetaDataBuffer.GetBytes(recordOffset + LABEL_OFFSET + BitUtil.SIZE_OF_INT, stringInBytes);

            return LabelCharset.GetString(stringInBytes);
        }
    }
}