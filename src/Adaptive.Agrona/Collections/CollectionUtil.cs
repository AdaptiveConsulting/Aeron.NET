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

namespace Adaptive.Agrona.Collections
{
    /// <summary>
    /// Utility functions for collection objects.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1118:Utility classes should not have public constructors",
        Justification = "Public ctor in shipped API surface; marking static would break consumers."
    )]
    public class CollectionUtil
    {
        /// <summary>
        /// A getOrDefault that doesn't create garbage if its suppler is non-capturing.
        /// </summary>
        /// <param name="map"> to perform the lookup on. </param>
        /// <param name="key"> on which the lookup is done. </param>
        /// <param name="supplier"> of the default value if one is not found. </param>
        /// <returns> the value if found or a new default which as been added to the map. </returns>
        public static TValue GetOrDefault<TKey, TValue>(
            IDictionary<TKey, TValue> map,
            TKey key,
            Func<TKey, TValue> supplier
        )
        {
            if (!map.TryGetValue(key, out TValue value))
            {
                value = supplier(key);
                map[key] = value;
            }

            return value;
        }
    }
}
