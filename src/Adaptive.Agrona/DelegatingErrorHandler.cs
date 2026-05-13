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

namespace Adaptive.Agrona
{
    /// <summary>
    /// <seealso cref="ErrorHandler"/> that can insert into a chain of responsibility so it handles an error and then
    /// delegates on to the next in the chain. This allows for taking action pre or post invocation of the next
    /// delegate.
    /// <para>
    /// Implementations are responsible for calling the next in the chain.
    /// </para>
    /// </summary>
    // ReSharper disable once InconsistentNaming -- Java parity: no I-prefix on this interface
    public interface DelegatingErrorHandler : IErrorHandler
    {
        /// <summary>
        /// Set the next <seealso cref="ErrorHandler"/> to be called in a chain.
        /// </summary>
        /// <param name="errorHandler"> the next <seealso cref="ErrorHandler"/> to be called in a chain. </param>
        void Next(IErrorHandler errorHandler);
    }
}
