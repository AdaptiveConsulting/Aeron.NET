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

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
    /// or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.
    ///
    /// Within this callback reentrant calls to the <see cref="Aeron"/> client are not permitted and
    /// will result in undefined behaviour.
    /// </summary>
    /// <param name="buffer"> containing the data. </param>
    /// <param name="offset"> at which the data begins. </param>
    /// <param name="length"> of the data in bytes. </param>
    /// <param name="header"> representing the meta data for the data. </param>
    public delegate void FragmentHandler(IDirectBuffer buffer, int offset, int length, Header header);
}