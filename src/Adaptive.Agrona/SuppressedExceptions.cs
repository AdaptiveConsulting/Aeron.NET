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

using System;
using System.Collections.Generic;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Extension methods that emulate Java's <c>Throwable.addSuppressed</c> / <c>getSuppressed</c> on .NET's
    /// <see cref="Exception"/> type via the <see cref="Exception.Data"/> dictionary.
    /// </summary>
    public static class SuppressedExceptions
    {
        private const string SuppressedKey = "Adaptive.Agrona.SuppressedExceptions";

        public static void AddSuppressed(this Exception primary, Exception suppressed)
        {
            if (primary == null || suppressed == null || ReferenceEquals(primary, suppressed))
            {
                return;
            }

            if (!(primary.Data[SuppressedKey] is List<Exception> list))
            {
                list = new List<Exception>();
                primary.Data[SuppressedKey] = list;
            }

            list.Add(suppressed);
        }

        public static IReadOnlyList<Exception> GetSuppressed(this Exception primary)
        {
            if (primary?.Data[SuppressedKey] is List<Exception> list)
            {
                return list;
            }

            return Array.Empty<Exception>();
        }
    }
}
