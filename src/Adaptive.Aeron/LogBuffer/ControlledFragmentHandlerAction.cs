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
    public enum ControlledFragmentHandlerAction
    {
        /// <summary>
        /// Abort the current polling operation and do not advance the position for this fragment.
        /// </summary>
        ABORT,

        /// <summary>
        /// Break from the current polling operation and commit the position as of the end of the current fragment
        /// being handled.
        /// </summary>
        BREAK,

        /// <summary>
        /// Continue processing but commit the position as of the end of the current fragment so that
        /// flow control is applied to this point.
        /// </summary>
        COMMIT,

        /// <summary>
        /// Continue processing until fragment limit or no fragments with position commit at end of poll as the in
        /// <seealso cref="FragmentHandler"/>.
        /// </summary>
        CONTINUE,
    }
}