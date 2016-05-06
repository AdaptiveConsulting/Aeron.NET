using System;
using System.Collections.Generic;

namespace Adaptive.Agrona.Collections
{
    /// <summary>
    /// Utility functions for collection objects.
    /// </summary>
    public class CollectionUtil
    {
        /// <summary>
        /// A getOrDefault that doesn't create garbage if its suppler is non-capturing.
        /// </summary>
        /// <param name="map"> to perform the lookup on. </param>
        /// <param name="key"> on which the lookup is done. </param>
        /// <param name="supplier"> of the default value if one is not found. </param>
        /// <returns> the value if found or a new default which as been added to the map. </returns>
        public static V GetOrDefault<K, V>(IDictionary<K, V> map, K key, Func<K, V> supplier)
        {
            V value;
            if (!map.TryGetValue(key, out value))
            {
                value = supplier(key);
                map[key] = value;
            }

            return value;
        }
    }
}