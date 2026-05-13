/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

using Adaptive.Agrona;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message for removing a Publication.
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                          Client ID                            |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                    Command Correlation ID                     |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                       Registration ID                         |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                           Flags                               |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public class RemovePublicationFlyweight : RemoveMessageFlyweight
    {
        private static readonly int FlagsFieldOffset = RegistrationIdFieldOffset + SIZE_OF_LONG;

        private const long FlagRevoke = 0x1;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public new RemovePublicationFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            base.Wrap(buffer, offset);

            return this;
        }

        /// <summary>
        /// Length of the message in bytes.
        /// </summary>
        /// <returns> length of the message in bytes. </returns>
        public static new int Length()
        {
            return RemoveMessageFlyweight.Length() + SIZE_OF_LONG;
        }

        /// <summary>
        /// Whether or not the message contains the flags field.
        /// </summary>
        /// <param name="messageLength"> the length of the message. </param>
        /// <returns> true if the flags field can be read. </returns>
        public bool FlagsFieldIsValid(int messageLength)
        {
            return messageLength >= FlagsFieldOffset + SIZE_OF_LONG;
        }

        /// <summary>
        /// Get the value of the revoke field.
        /// </summary>
        /// <returns> revoked. </returns>
        public bool Revoke()
        {
            return (_buffer.GetLong(_offset + FlagsFieldOffset) & FlagRevoke) > 0;
        }

        /// <summary>
        /// Whether or not the message contains the set revoke flag.
        /// </summary>
        /// <param name="messageLength"> the length of the message. </param>
        /// <returns> true if the flags field is present AND the revoked flag is set. </returns>
        public bool Revoke(int messageLength)
        {
            return FlagsFieldIsValid(messageLength) && Revoke();
        }

        /// <summary>
        /// Set the value of the revoke field.
        /// </summary>
        /// <param name="revoke"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public RemovePublicationFlyweight Revoke(bool revoke)
        {
            long flags = _buffer.GetLong(_offset + FlagsFieldOffset);

            if (revoke)
            {
                flags |= FlagRevoke;
            }
            else
            {
                flags &= ~FlagRevoke;
            }

            _buffer.PutLong(_offset + FlagsFieldOffset, flags);

            return this;
        }
    }
}
