using System.Collections.Generic;

namespace Adaptive.Agrona.Collections
{
    public static class DictionaryExtensions
    {
        public static TValue GetOrDefault<TKey, TValue>(
            this IDictionary<TKey, TValue> dictionary,
            TKey key,
            TValue @default = default(TValue))
        {
            return dictionary.TryGetValue(key, out var value) ? value : @default;
        }
    }
}