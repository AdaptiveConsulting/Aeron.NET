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

using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                            Client ID                          |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                         Correlation ID                        |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// </summary>
    public class CorrelatedMessageFlyweight
    {
        public const int CLIENT_ID_FIELD_OFFSET = 0;
        public static readonly int CORRELATION_ID_FIELD_OFFSET = CLIENT_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int LENGTH = 2 * BitUtil.SIZE_OF_LONG;

        protected internal IMutableDirectBuffer buffer;
        protected internal int offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> for fluent API </returns>
        public CorrelatedMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            this.buffer = buffer;
            this.offset = offset;

            return this;
        }

        /// <summary>
        /// return client id field
        /// </summary>
        /// <returns> client id field </returns>
        public long ClientId()
        {
            return buffer.GetLong(offset + CLIENT_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set client id field
        /// </summary>
        /// <param name="clientId"> field value </param>
        /// <returns> for fluent API </returns>
        public CorrelatedMessageFlyweight ClientId(long clientId)
        {
            buffer.PutLong(offset + CLIENT_ID_FIELD_OFFSET, clientId);

            return this;
        }

        /// <summary>
        /// return correlation id field
        /// </summary>
        /// <returns> correlation id field </returns>
        public long CorrelationId()
        {
            return buffer.GetLong(offset + CORRELATION_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set correlation id field
        /// </summary>
        /// <param name="correlationId"> field value </param>
        /// <returns> for fluent API </returns>
        public CorrelatedMessageFlyweight CorrelationId(long correlationId)
        {
            buffer.PutLong(offset + CORRELATION_ID_FIELD_OFFSET, correlationId);

            return this;
        }
    }

}