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

using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message for removing a Publication or Subscription.
    /// 
    /// <para>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                            Client ID                          |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                    Command Correlation ID                     |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                         Registration ID                       |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// </para>
    /// </summary>
    public class RemoveMessageFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int REGISTRATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int MINIMUM_LENGTH = REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public new RemoveMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            base.Wrap(buffer, offset);

            return this;
        }
        
        /// <summary>
        /// Get the registration id field.
        /// </summary>
        /// <returns> registration id field. </returns>
        public long RegistrationId()
        {
            return buffer.GetLong(offset + REGISTRATION_ID_OFFSET);
        }

        /// <summary>
        /// Set registration  id field.
        /// </summary>
        /// <param name="registrationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public RemoveMessageFlyweight RegistrationId(long registrationId)
        {
            buffer.PutLong(offset + REGISTRATION_ID_OFFSET, registrationId);

            return this;
        }

        /// <summary>
        /// Length of the message in bytes.
        /// </summary>
        /// <returns> length of the message in bytes. </returns>
        public static int Length()
        {
            return LENGTH + BitUtil.SIZE_OF_LONG;
        }
    }
}