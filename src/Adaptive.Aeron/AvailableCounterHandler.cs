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

using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for notification of<seealso cref="Counter"/>s becoming available via a <seealso cref="Aeron"/> client.
    ///
    /// Method called by Aeron to deliver notification of a {@link Counter} being available.
    ///
    /// Within this callback reentrant calls to the <see cref="Aeron"/> client are not permitted and will result in
    /// undefined behaviour.
    ///
    /// </summary>
    /// <param name="countersReader"> for more detail on the counter. </param>
    /// <param name="registrationId"> for the counter. </param>
    /// <param name="counterId">      that is available. </param>
    public delegate void AvailableCounterHandler(CountersReader countersReader, long registrationId, int counterId);
}
