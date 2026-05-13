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

using System.Runtime.CompilerServices;
using Adaptive.Aeron.Exceptions;

namespace Adaptive.Aeron
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1118:Utility classes should not have public constructors",
        Justification = "Public ctor in shipped API surface; marking static would break consumers."
    )]
    public class AeronThrowHelper
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowAeronException(string message)
        {
            throw GetAeronException(message);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static AeronException GetAeronException(string message)
        {
            return new AeronException(message);
        }
    }
}
