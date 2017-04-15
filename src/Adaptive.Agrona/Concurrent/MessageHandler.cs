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

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Callback interface for processing of messages that are read from a buffer.
    /// Called for the processing of each message read from a buffer in turn.
    /// </summary>
    /// <param name="msgTypeId"> type of the encoded message.</param>
    /// <param name="buffer"> containing the encoded message.</param>
    /// <param name="index"> at which the encoded message begins.</param>
    /// <param name="length"> in bytes of the encoded message.</param>
    public delegate void MessageHandler(int msgTypeId, IMutableDirectBuffer buffer, int index, int length);
}