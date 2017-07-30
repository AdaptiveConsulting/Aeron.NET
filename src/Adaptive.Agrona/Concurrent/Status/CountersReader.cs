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
    ///  |                      120 bytes for key                       ...
    /// ...                                                              |
    ///  +-+-------------------------------------------------------------+
    ///  |R|                      Label Length                           |
    ///  +-+-------------------------------------------------------------+
    ///  |                  380 bytes of Label in UTF-8                 ...
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
        /// Record has not been used.
        /// </summary>
        public const int RECORD_UNUSED = 0;

        /// <summary>
        /// Record currently allocated for use..
        /// </summary>
        public const int RECORD_ALLOCATED = 1;

        /// <summary>
        /// Record was active and now has been reclaimed.
        /// </summary>
        public const int RECORD_RECLAIMED = -1;

        /// <summary>
        /// Offset in the record at which the type id field is stored.
        /// </summary>
        public static readonly int TYPE_ID_OFFSET = BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Offset in the record at which the key is stored.
        /// </summary>
        public static readonly int KEY_OFFSET = TYPE_ID_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Offset in the record at which the label is stored.
        /// </summary>
        public static readonly int LABEL_OFFSET = BitUtil.CACHE_LINE_LENGTH*2;

        /// <summary>
        /// Length of a counter label length including length prefix.
        /// </summary>
        public static readonly int FULL_LABEL_LENGTH = BitUtil.CACHE_LINE_LENGTH*6;

        /// <summary>
        /// Maximum length of a label not including its length prefix.
        /// </summary>
        public static readonly int MAX_LABEL_LENGTH = FULL_LABEL_LENGTH - BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Maximum length a key can be.
        /// </summary>
        public static readonly int MAX_KEY_LENGTH = (BitUtil.CACHE_LINE_LENGTH*2) - (BitUtil.SIZE_OF_INT*2);
        
        /// <summary>
        /// Length of a meta data record in bytes.
        /// </summary>
        public static readonly int METADATA_LENGTH = LABEL_OFFSET + FULL_LABEL_LENGTH;

        /// <summary>
        /// Length of the space allocated to a counter that includes padding to avoid false sharing.
        /// </summary>
        public static readonly int COUNTER_LENGTH = BitUtil.CACHE_LINE_LENGTH*2;

        /// <summary>
        /// Construct a reader over buffers containing the values and associated metadata.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the counter metadata. </param>
        /// <param name="valuesBuffer">   containing the counter values. </param>
        public CountersReader(IAtomicBuffer metaDataBuffer, IAtomicBuffer valuesBuffer)
        {
            ValuesBuffer = valuesBuffer;
            MetaDataBuffer = metaDataBuffer;
        }

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
        /// The offset in the counter buffer for a given counterId.
        /// </summary>
        /// <param name="counterId"> for which the offset should be provided. </param>
        /// <returns> the offset in the counter buffer. </returns>
        public static int CounterOffset(int counterId)
        {
            return counterId*COUNTER_LENGTH;
        }

        /// <summary>
        /// The offset in the metadata buffer for a given id.
        /// </summary>
        /// <param name="counterId"> for the record. </param>
        /// <returns> the offset at which the metadata record begins. </returns>
        public static int MetaDataOffset(int counterId)
        {
            return counterId*METADATA_LENGTH;
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
                if (RECORD_UNUSED == recordStatus)
                {
                    break;
                }
                else if (RECORD_ALLOCATED == recordStatus)
                {
                    var label = MetaDataBuffer.GetStringUtf8(i + LABEL_OFFSET);
                    consumer(counterId, label);
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
                if (RECORD_UNUSED == recordStatus)
                {
                    break;
                }
                if (RECORD_ALLOCATED == recordStatus)
                {
                    var typeId = MetaDataBuffer.GetInt(i + TYPE_ID_OFFSET);
                    var label = MetaDataBuffer.GetStringUtf8(i + LABEL_OFFSET);
                    IDirectBuffer keyBuffer = new UnsafeBuffer(MetaDataBuffer, i + KEY_OFFSET, MAX_KEY_LENGTH);

                    metaData(counterId, typeId, keyBuffer, label);
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
            return ValuesBuffer.GetLongVolatile(CounterOffset(counterId));
        }

        /// <summary>
        /// Callback function for consuming metadata records of counters.
        /// </summary>
        public delegate void MetaData(int counterId, int typeId, IDirectBuffer keyBuffer, string label);
    }
}