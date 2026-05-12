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

namespace Adaptive.Agrona.SBE
{
    /// <summary>
    /// Common behaviour to SBE Message encoder and decoder flyweights.
    /// </summary>
    public interface IMessageFlyweight : IFlyweight
    {
        /// <summary>
        /// The length of the root block in bytes.
        /// </summary>
        /// <returns> the length of the root block in bytes. </returns>
        int SbeBlockLength();

        /// <summary>
        /// The SBE template identifier for the message.
        /// </summary>
        /// <returns> the SBE template identifier for the message. </returns>
        int SbeTemplateId();

        /// <summary>
        /// The SBE Schema identifier containing the message declaration.
        /// </summary>
        /// <returns> the SBE Schema identifier containing the message declaration. </returns>
        int SbeSchemaId();

        /// <summary>
        /// The version number of the SBE Schema containing the message.
        /// </summary>
        /// <returns> the version number of the SBE Schema containing the message. </returns>
        int SbeSchemaVersion();

        /// <summary>
        /// The semantic type of the message which is typically the semantic equivalent in the FIX repository.
        /// </summary>
        /// <returns> the semantic type of the message which is typically the semantic equivalent in the FIX repository.
        /// </returns>
        string SbeSemanticType();

        /// <summary>
        /// The current offset in the buffer from which the message is being encoded or decoded.
        /// </summary>
        /// <returns> the current offset in the buffer from which the message is being encoded or decoded. </returns>
        int Offset();
    }
}
