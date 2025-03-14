using System;
using System.Collections;
using System.Collections.Generic;

namespace Adaptive.Agrona.Collections
{
    /// <summary>
    /// A map implementation that replicates the behaviour of the Java equivalent.
    /// </summary>
    /// <typeparam name="TKey">The type of the keys in the map.</typeparam>
    /// <typeparam name="TValue">The type of the values in the map.</typeparam>
    public class Map<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        private readonly IDictionary<TKey, TValue> _dictionaryImplementation = new Dictionary<TKey, TValue>();
        private readonly TValue _defaultValue;

        public Map(TValue defaultValue = default)
        {
            _defaultValue = defaultValue;
        }

        public void Clear()
        {
            _dictionaryImplementation.Clear();
        }

        public TValue Put(TKey key, TValue value)
        {
            var oldValue = Get(key);

            _dictionaryImplementation[key] = value;
            return oldValue;
        }

        public TValue Remove(TKey key)
        {
            if (!_dictionaryImplementation.TryGetValue(key, out var value)) return _defaultValue;
            _dictionaryImplementation.Remove(key);
            return value;
        }

        public TValue Get(TKey key)
        {
            return _dictionaryImplementation.TryGetValue(key, out var value) ? value : _defaultValue;
        }

        public ICollection<TValue> Values => _dictionaryImplementation.Values;

        public ICollection<KeyValuePair<TKey, TValue>> KeyValuePairs => _dictionaryImplementation;

        public int Count => _dictionaryImplementation.Count;

        public bool ContainsKey(TKey key)
        {
            return _dictionaryImplementation.ContainsKey(key);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _dictionaryImplementation.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void ForEach(Action<TKey, TValue> consumer)
        {
            foreach (var keyValuePair in _dictionaryImplementation)
            {
                consumer(keyValuePair.Key, keyValuePair.Value);
            }
        }
    }
}